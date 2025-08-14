import datetime as dt
import logging
import os
import sys
import time
import json
import traceback
from typing import List, Tuple, Dict, Any, Optional

import boto3
import pytz
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError, BotoCoreError

# Configuración de logging mejorada
def setup_logging():
    """Configura el sistema de logging con formato detallado y manejo de errores"""
    log_level = os.environ.get("LOGGING", "INFO").upper()
    
    # Crear formateador personalizado
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Configurar handler para consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Configurar logger principal
    logger = logging.getLogger("CrawlerStage")
    logger.setLevel(getattr(logging, log_level, logging.INFO))
    logger.addHandler(console_handler)
    
    # Evitar duplicación de logs
    logger.propagate = False
    
    return logger

logger = setup_logging()

# Constantes de configuración
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos
TIMEOUT_SECONDS = 300  # 5 minutos

class CrawlerStageError(Exception):
    """Excepción personalizada para errores del CrawlerStage"""
    pass

def validate_arguments(args: Dict[str, str]) -> None:
    """Valida que todos los argumentos requeridos estén presentes"""
    required_args = [
        'S3_STAGE_BUCKET', 'DYNAMO_LOGS_TABLE', 
        'PROCESS_ID', 'ENDPOINT_NAME', 'TEAM', 'DATA_SOURCE', 
        'REGION', 'ENVIRONMENT', 'CRAWLER_CONFIG', 'ARN_ROLE_CRAWLER'
    ]
    
    missing_args = [arg for arg in required_args if not args.get(arg)]
    if missing_args:
        raise CrawlerStageError(f"Argumentos faltantes: {', '.join(missing_args)}")
    
    logger.info(f"Validación de argumentos completada exitosamente")
    logger.debug(f"Argumentos recibidos: {list(args.keys())}")

def retry_on_failure(max_retries: int = MAX_RETRIES, delay: int = RETRY_DELAY):
    """Decorador para reintentar operaciones que fallan"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    if attempt > 0:
                        logger.info(f"Reintento {attempt}/{max_retries} para {func.__name__}")
                        time.sleep(delay * attempt)  # Backoff exponencial
                    
                    return func(*args, **kwargs)
                    
                except (ClientError, BotoCoreError, Exception) as e:
                    last_exception = e
                    logger.warning(f"Intento {attempt + 1} falló para {func.__name__}: {str(e)}")
                    
                    if attempt == max_retries:
                        logger.error(f"Todos los reintentos fallaron para {func.__name__}")
                        break
            
            raise last_exception
        return wrapper
    return decorator

# Inicialización con validación
try:
    logger.info("Iniciando CrawlerStage...")
    
    args = getResolvedOptions(
        sys.argv, ['S3_STAGE_BUCKET', 'DYNAMO_LOGS_TABLE', 
                  'PROCESS_ID', 'ENDPOINT_NAME', 'TEAM', 'DATA_SOURCE', 
                  'REGION', 'ENVIRONMENT', 'CRAWLER_CONFIG', 'ARN_ROLE_CRAWLER']
    )
    
    validate_arguments(args)
    
    # Inicialización de clientes AWS con manejo de errores
    logger.info("Inicializando clientes AWS...")
    
    dynamodb = boto3.resource('dynamodb')
    dynamodb_client = boto3.client('dynamodb')  # Cliente adicional para describe_table
    client_glue = boto3.client('glue')
    client_lakeformation = boto3.client('lakeformation')
    
    logger.info("Clientes AWS inicializados correctamente")
    
except Exception as e:
    logger.critical(f"Error crítico durante la inicialización: {str(e)}")
    logger.critical(f"Traceback completo: {traceback.format_exc()}")
    sys.exit(1)

# Variables globales con logging
dynamo_logs_table = args['DYNAMO_LOGS_TABLE']
logger.info(f"Tabla de logs DynamoDB: {dynamo_logs_table}")

# Cargar configuración del crawler desde parámetros del job
try:
    crawler_config = json.loads(args['CRAWLER_CONFIG'])
    logger.info("Configuración de crawler cargada exitosamente desde parámetros")
    logger.debug(f"Configuración: {json.dumps(crawler_config, indent=2)}")
except Exception as e:
    logger.error(f"Error procesando la configuración del crawler: {str(e)}")
    raise CrawlerStageError(f"No se pudo procesar la configuración del crawler: {str(e)}")

s3_target = args['S3_STAGE_BUCKET']
arn_role_crawler = args['ARN_ROLE_CRAWLER']
endpoint_name = crawler_config.get('endpoint_name')

logger.info(f"Configuración establecida - Endpoint: {endpoint_name}, S3 Target: {s3_target}")

# Obtener datos del endpoint desde la configuración del job
try:
    logger.info(f"Usando datos del endpoint desde la configuración: {endpoint_name}")
    
    if not endpoint_name:
        raise CrawlerStageError("Endpoint no especificado en la configuración")
    
    # La información del endpoint viene en la configuración
    endpoint_data = crawler_config.get('endpoint_data', {})
    logger.info(f"Datos del endpoint obtenidos desde configuración: {endpoint_data.get('BD_TYPE', 'N/A')}")
    
except Exception as e:
    logger.error(f"Error procesando datos del endpoint: {str(e)}")
    raise CrawlerStageError(f"No se pudieron obtener los datos del endpoint: {str(e)}")

data_catalog_database_name = f"{args['TEAM']}_{args['DATA_SOURCE']}_{endpoint_name}_stage".lower()
data_catalog_crawler_name = data_catalog_database_name + "_cw"

logger.info(f"Nombres generados - Database: {data_catalog_database_name}, Crawler: {data_catalog_crawler_name}")

@retry_on_failure()
def create_database_data_catalog(database_data_catalog_name: str) -> bool:
    """Crea una base de datos en el catálogo de datos de Glue"""
    logger.info(f"Creando base de datos en catálogo: {database_data_catalog_name}")
    
    try:
        response = client_glue.create_database(
            DatabaseInput={
                'Name': database_data_catalog_name,
                'Description': f'Database for {endpoint_name} stage data'
            }
        )
        logger.info(f"Base de datos '{database_data_catalog_name}' creada exitosamente")
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AlreadyExistsException':
            logger.warning(f"La base de datos '{database_data_catalog_name}' ya existe")
            return True
        else:
            logger.error(f"Error de cliente AWS creando base de datos: {error_code} - {str(e)}")
            raise
    except Exception as e:
        logger.error(f"Error inesperado creando base de datos: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

@retry_on_failure()
def get_database_data_catalog(database_data_catalog_name: str) -> bool:
    """Verifica si existe una base de datos en el catálogo"""
    logger.info(f"Verificando existencia de base de datos: {database_data_catalog_name}")
    
    try:
        response = client_glue.get_database(Name=database_data_catalog_name)
        logger.info(f"Base de datos '{database_data_catalog_name}' encontrada")
        logger.debug(f"Detalles de la base de datos: {response.get('Database', {}).get('Name', 'N/A')}")
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'EntityNotFoundException':
            logger.info(f"Base de datos '{database_data_catalog_name}' no existe")
            return False
        else:
            logger.error(f"Error verificando base de datos: {error_code} - {str(e)}")
            raise
    except Exception as e:
        logger.error(f"Error inesperado verificando base de datos: {str(e)}")
        return False

@retry_on_failure()
def grant_permissions_to_database_lakeformation(job_role_arn_name: str, database_data_catalog_name: str) -> None:
    """Otorga permisos a la base de datos en Lake Formation"""
    logger.info(f"Otorgando permisos de base de datos en Lake Formation")
    logger.info(f"Principal: {job_role_arn_name}")
    logger.info(f"Database: {database_data_catalog_name}")
    
    try:
        response = client_lakeformation.grant_permissions(
            Principal={
                'DataLakePrincipalIdentifier': job_role_arn_name
            },
            Resource={
                'Database': {
                    'Name': database_data_catalog_name
                },
            },
            Permissions=['ALL'],
            PermissionsWithGrantOption=['ALL']
        )
        logger.info("Permisos de base de datos otorgados exitosamente")
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AlreadyExistsException':
            logger.warning("Los permisos ya existen para esta base de datos")
        else:
            logger.error(f"Error otorgando permisos de base de datos: {error_code} - {str(e)}")
            raise
    except Exception as e:
        logger.error(f"Error inesperado otorgando permisos de base de datos: {str(e)}")
        raise
    
@retry_on_failure()
def create_lf_tag_if_not_exists(tag_key: str, tag_values: List[str]) -> bool:
    """Crea un LF-Tag si no existe"""
    logger.info(f"Verificando/creando LF-Tag: {tag_key} con valores: {tag_values}")
    
    try:
        # Verificar si el tag existe
        response = client_lakeformation.get_lf_tag(TagKey=tag_key)
        existing_values = response['TagValues']
        logger.info(f"LF-Tag '{tag_key}' ya existe con valores: {existing_values}")
        
        # Verificar si necesitamos agregar nuevos valores
        missing_values = [val for val in tag_values if val not in existing_values]
        if missing_values:
            logger.info(f"Actualizando LF-Tag con nuevos valores: {missing_values}")
            all_values = list(set(existing_values + tag_values))
            client_lakeformation.update_lf_tag(
                TagKey=tag_key,
                TagValuesToAdd=missing_values
            )
            logger.info(f"LF-Tag actualizado exitosamente")
        
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'EntityNotFoundException':
            # El tag no existe, crearlo
            logger.info(f"LF-Tag '{tag_key}' no existe, creándolo...")
            try:
                response = client_lakeformation.create_lf_tag(
                    TagKey=tag_key,
                    TagValues=tag_values
                )
                logger.info(f"LF-Tag '{tag_key}' creado exitosamente")
                return True
            except Exception as create_error:
                logger.error(f"Error creando LF-Tag: {str(create_error)}")
                raise
        else:
            logger.error(f"Error verificando LF-Tag: {error_code} - {str(e)}")
            raise
    except Exception as e:
        logger.error(f"Error inesperado con LF-Tag: {str(e)}")
        raise

@retry_on_failure()
def grant_permissions_lf_tag_lakeformation(job_role_arn_name: str) -> None:
    """Otorga permisos de LF-Tag en Lake Formation"""
    logger.info(f"Otorgando permisos de LF-Tag para el rol: {job_role_arn_name}")
    
    try:
        # Grant permissions to manage and use the LF-Tag
        response = client_lakeformation.grant_permissions(
            Principal={
                'DataLakePrincipalIdentifier': job_role_arn_name
            },
            Resource={
                'LFTag': {
                    'TagKey': 'Level',
                    'TagValues': ['Stage']
                },
            },
            Permissions=['ASSOCIATE', 'DESCRIBE'],
            PermissionsWithGrantOption=['ASSOCIATE', 'DESCRIBE']
        )
        logger.info("Permisos de LF-Tag otorgados exitosamente")
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AlreadyExistsException':
            logger.warning("Los permisos de LF-Tag ya existen")
        else:
            logger.error(f"Error otorgando permisos de LF-Tag: {error_code} - {str(e)}")
            raise
    except Exception as e:
        logger.error(f"Error inesperado otorgando permisos de LF-Tag: {str(e)}")
        raise

@retry_on_failure()
def check_lf_tags_on_database(database_data_catalog_name: str) -> bool:
    """Verifica si la base de datos ya tiene LF-Tags asignados"""
    logger.info(f"Verificando LF-Tags en la base de datos: {database_data_catalog_name}")
    
    try:
        response = client_lakeformation.get_lf_tags_for_resource(
            Resource={
                'Database': {
                    'Name': database_data_catalog_name
                }
            }
        )
        
        lf_tags = response.get('LFTagOnDatabase', [])
        if lf_tags:
            logger.info(f"Base de datos ya tiene LF-Tags: {lf_tags}")
            # Verificar si tiene el tag 'Level' con valor 'Stage'
            for tag in lf_tags:
                if tag.get('TagKey') == 'Level' and 'Stage' in tag.get('TagValues', []):
                    logger.info("Base de datos ya tiene el LF-Tag 'Level: Stage'")
                    return True
            logger.info("Base de datos tiene LF-Tags pero no el requerido 'Level: Stage'")
            return False
        else:
            logger.info("Base de datos no tiene LF-Tags asignados")
            return False
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'EntityNotFoundException':
            logger.info("Base de datos no encontrada o sin LF-Tags")
            return False
        else:
            logger.warning(f"Error verificando LF-Tags: {error_code} - {str(e)}")
            return False
    except Exception as e:
        logger.warning(f"Error inesperado verificando LF-Tags: {str(e)}")
        return False

@retry_on_failure()
def add_lf_tags_to_database_lakeformation(database_data_catalog_name: str) -> None:
    """Agrega LF-Tags a la base de datos"""
    logger.info(f"Agregando LF-Tags a la base de datos: {database_data_catalog_name}")
    
    try:
        response = client_lakeformation.add_lf_tags_to_resource(
            Resource={
                'Database': {
                    'Name': database_data_catalog_name
                },
            },
            LFTags=[
                {
                    'TagKey': 'Level',
                    'TagValues': ['Stage']
                }
            ]
        )
        logger.info("LF-Tags agregados exitosamente a la base de datos")
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AlreadyExistsException':
            logger.warning("LF-Tags ya existen en la base de datos")
        else:
            logger.error(f"Error agregando LF-Tags: {error_code} - {str(e)}")
            raise
    except Exception as e:
        logger.error(f"Error inesperado agregando LF-Tags: {str(e)}")
        raise

def build_crawler_targets(total_list: List[str]) -> List[Dict[str, Any]]:
    """Construye la lista de targets para el crawler"""
    logger.info(f"Construyendo targets para {len(total_list)} tablas")
    tables = []
    
    # Obtener la información de las tablas desde la configuración
    tables_config = crawler_config.get('tables', [])
    # Crear un mapa para acceso rápido
    tables_map = {table_conf['table_name']: table_conf for table_conf in tables_config if 'table_name' in table_conf}
    
    for table_name in total_list:
        try:
            logger.debug(f"Procesando tabla: {table_name}")
            
            # Construir ruta S3 - usar datos de configuración si están disponibles, 
            # de lo contrario usar el nombre de tabla directamente
            if table_name in tables_map:
                table_data = tables_map[table_name]
                stage_table_name = table_data.get('stage_table_name', table_name)
            else:
                # Si no hay configuración específica, usar el nombre de tabla directamente
                stage_table_name = table_name
                logger.debug(f"Tabla '{table_name}' no encontrada en configuración, usando nombre directo")
            
            # Construir ruta S3 usando el patrón estándar que usa light_transform
            # Patrón: s3://bucket/team/data_source/endpoint_name/table_name/
            s3_path = f"s3://{s3_target}/{args['TEAM']}/{args['DATA_SOURCE']}/{args['ENDPOINT_NAME']}/{stage_table_name}/"
            
            data_source = {
                'DeltaTables': [s3_path],
                'ConnectionName': '',
                'WriteManifest': True
            }
            
            tables.append(data_source)
            logger.debug(f"Target agregado para tabla '{table_name}': {s3_path}")
            
        except Exception as e:
            logger.error(f"Error procesando tabla '{table_name}': {str(e)}")
            continue
    
    logger.info(f"Se construyeron {len(tables)} targets exitosamente")
    return tables

@retry_on_failure()
def create_crawler(total_list: List[str]) -> bool:
    """Crea un nuevo crawler en Glue"""
    logger.info(f"Creando crawler: {data_catalog_crawler_name}")
    
    try:
        tables = build_crawler_targets(total_list)
        
        if not tables:
            logger.warning("No se encontraron tablas válidas para el crawler")
            return False
        
        response = client_glue.create_crawler(
            Name=data_catalog_crawler_name,
            Role=arn_role_crawler,
            DatabaseName=data_catalog_database_name,
            Description=f'Crawler for {endpoint_name} stage tables',
            Targets={
                'DeltaTargets': tables
            },
            Configuration=json.dumps({
                "Version": 1.0,
                "CrawlerOutput": {
                    "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"}
                }
            })
        )
        
        logger.info(f"Crawler '{data_catalog_crawler_name}' creado exitosamente")
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AlreadyExistsException':
            logger.warning(f"El crawler '{data_catalog_crawler_name}' ya existe")
            return True
        else:
            logger.error(f"Error creando crawler: {error_code} - {str(e)}")
            raise
    except Exception as e:
        logger.error(f"Error inesperado creando crawler: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

@retry_on_failure()
def edit_crawler(total_list: List[str]) -> bool:
    """Actualiza la configuración de un crawler existente"""
    logger.info(f"Actualizando crawler: {data_catalog_crawler_name}")
    
    try:
        tables = build_crawler_targets(total_list)
        
        if not tables:
            logger.warning("No se encontraron tablas válidas para actualizar el crawler")
            return False
        
        response = client_glue.update_crawler(
            Name=data_catalog_crawler_name,
            Role=arn_role_crawler,
            DatabaseName=data_catalog_database_name,
            Description=f'Updated crawler for {endpoint_name} stage tables',
            Targets={
                'DeltaTargets': tables
            }
        )
        
        logger.info(f"Crawler '{data_catalog_crawler_name}' actualizado exitosamente")
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        logger.error(f"Error actualizando crawler: {error_code} - {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error inesperado actualizando crawler: {str(e)}")
        raise

@retry_on_failure()
def get_crawler(crawler_name: str) -> bool:
    """Verifica si existe un crawler"""
    logger.info(f"Verificando existencia del crawler: {crawler_name}")
    
    try:
        response = client_glue.get_crawler(Name=crawler_name)
        crawler_state = response['Crawler']['State']
        logger.info(f"Crawler '{crawler_name}' encontrado, estado: {crawler_state}")
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'EntityNotFoundException':
            logger.info(f"Crawler '{crawler_name}' no existe")
            return False
        else:
            logger.error(f"Error verificando crawler: {error_code} - {str(e)}")
            raise
    except Exception as e:
        logger.error(f"Error inesperado verificando crawler: {str(e)}")
        return False

@retry_on_failure()
def start_crawler(crawler_name: str) -> bool:
    """Inicia la ejecución de un crawler"""
    logger.info(f"Iniciando crawler: {crawler_name}")
    
    try:
        # Verificar estado actual del crawler
        response = client_glue.get_crawler(Name=crawler_name)
        current_state = response['Crawler']['State']
        
        if current_state == 'RUNNING':
            logger.info(f"El crawler '{crawler_name}' ya está ejecutándose")
            return True
        elif current_state in ['STOPPING', 'READY']:
            logger.info(f"Estado del crawler: {current_state}, procediendo a iniciar...")
        else:
            logger.warning(f"Estado inesperado del crawler: {current_state}")
        
        # Iniciar crawler
        start_response = client_glue.start_crawler(Name=crawler_name)
        logger.info(f"Crawler '{crawler_name}' iniciado exitosamente")
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'CrawlerRunningException':
            logger.info(f"El crawler '{crawler_name}' ya está ejecutándose")
            return True
        else:
            logger.error(f"Error iniciando crawler: {error_code} - {str(e)}")
            raise
    except Exception as e:
        logger.error(f"Error inesperado iniciando crawler: {str(e)}")
        raise

@retry_on_failure()
def update_attribute_value_dynamodb(row_key_field_name: str, row_key: str, 
                                   attribute_name: str, attribute_value: Any, 
                                   table_name: str) -> bool:
    """Actualiza un atributo en DynamoDB"""
    logger.info(f'Actualizando DynamoDB - Tabla: {table_name}, Key: {row_key}, Atributo: {attribute_name}')
    
    try:
        dynamo_table = dynamodb.Table(table_name)
        response = dynamo_table.update_item(
            Key={row_key_field_name: row_key},
            AttributeUpdates={
                attribute_name: {
                    'Value': attribute_value,
                    'Action': 'PUT'
                }
            },
            ReturnValues='UPDATED_NEW'
        )
        
        logger.info(f'Atributo actualizado exitosamente en DynamoDB: {row_key}')
        logger.debug(f'Respuesta de DynamoDB: {response.get("Attributes", {})}')
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        logger.error(f"Error de cliente DynamoDB: {error_code} - {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error inesperado actualizando DynamoDB: {str(e)}")
        raise

def get_tables_from_s3() -> List[str]:
    """Descubre tablas automáticamente desde el bucket S3 stage"""
    logger.info(f"Escaneando S3 para descubrir tablas en: {s3_target}")
    
    try:
        # Construir el prefijo de búsqueda
        team = args['TEAM']
        data_source = args['DATA_SOURCE']
        endpoint_name = args['ENDPOINT_NAME']
        
        # El patrón esperado es: s3://bucket/team/data_source/endpoint_name/table_name/
        # (ahora light_transform incluye endpoint_name en el path stage)
        search_prefix = f"{team}/{data_source}/{endpoint_name}/"
        logger.info(f"Buscando tablas con prefijo: {search_prefix}")
         
        bucket_name = s3_target 
        logger.info(f"Bucket S3: {bucket_name}") 
        s3_client = boto3.client('s3', region_name=args['REGION']) 
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=search_prefix, Delimiter='/') 
        tables = set()
        for page in pages:
            # Obtener "folders" que representan tablas
            for common_prefix in page.get('CommonPrefixes', []):
                prefix = common_prefix['Prefix']
                # Extraer el nombre de la tabla del path
                # Formato esperado: team/data_source/endpoint_name/table_name/
                parts = prefix.rstrip('/').split('/')
                if len(parts) >= 4:  # Asegurar que tenemos suficientes partes (team/data_source/endpoint_name/table_name)
                    table_name = parts[3]  # El nombre de la tabla está en la 4ta posición
                    tables.add(table_name)
                    logger.debug(f"Tabla encontrada: {table_name}")
        
        table_list = list(tables)
        logger.info(f"Se encontraron {len(table_list)} tablas en S3: {table_list}")
        
        return table_list
        
    except Exception as e:
        logger.error(f"Error escaneando S3 para descubrir tablas: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        # Retornar lista vacía en caso de error, pero no fallar el proceso
        return []

def get_crawler_status_from_config(config: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """Obtiene el estado del crawler para las tablas desde la configuración o escaneando S3"""
    logger.info(f"Obteniendo estado del crawler desde configuración para endpoint: {endpoint_name}")
    
    total_list = []
    empty_table = []
    processed_count = 0
    error_count = 0
    
    try:
        # Intentar obtener tablas de la configuración primero
        tables_config = config.get('tables', [])
        logger.info(f"Se encontraron {len(tables_config)} tablas en la configuración")
        
        # Si no hay tablas en la configuración, escanear S3
        if not tables_config:
            logger.info("No se encontraron tablas en la configuración, escaneando S3...")
            s3_tables = get_tables_from_s3()
            
            if s3_tables:
                logger.info(f"Se encontraron {len(s3_tables)} tablas en S3: {s3_tables}")
                # Convertir a formato esperado por el resto del código
                for table_name in s3_tables:
                    total_list.append(table_name)
                    empty_table.append(table_name)
                    
                logger.info(f"Procesamiento desde S3 completado - Total: {len(total_list)}, Sin crawler: {len(empty_table)}")
                return total_list, empty_table
            else:
                logger.warning("No se encontraron tablas ni en configuración ni en S3")
                return [], []
        
        # Procesar tablas desde configuración (lógica original)
        for table_config in tables_config:
            try:
                processed_count += 1
                
                # Validar la estructura del elemento
                if 'table_name' not in table_config:
                    logger.debug("Elemento sin table_name encontrado, saltando...")
                    continue
                
                table_name = table_config['table_name']
                
                # Para el registro en logs usamos el mismo patrón que antes
                total_list.append(table_name)
                empty_table.append(table_name)
                logger.debug(f"Tabla procesada: {table_name}")

            except Exception as e:
                error_count += 1
                table_name = table_config.get('table_name', 'UNKNOWN')
                logger.error(f"Error procesando tabla '{table_name}': {str(e)}")
                continue

        logger.info(f"Procesamiento completado - Total: {len(total_list)}, Sin crawler: {len(empty_table)}, Errores: {error_count}")
        
        # Solo registramos en logs, pero no necesitamos actualizar el estado
        logger.info(f"Tablas a procesar: {total_list}")
        
        return total_list, empty_table

    except Exception as e:
        logger.error(f"Error crítico procesando configuración del crawler: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise CrawlerStageError(f"No se pudo procesar la configuración del crawler: {str(e)}")

# Función principal con manejo robusto de errores
def main():
   """Función principal del proceso"""
   start_time = time.time()
   logger.info("="*60)
   logger.info("INICIANDO PROCESO CRAWLER STAGE")
   logger.info("="*60)
   
   try:
       # Obtener estado de las tablas desde la configuración
       logger.info("Paso 1: Obteniendo estado de las tablas...")
       total_list, empty_table = get_crawler_status_from_config(crawler_config)
       
       if not total_list:
           logger.warning(f"No se encontraron tablas para el endpoint '{endpoint_name}'")
           logger.info("Proceso finalizado - No hay tablas para procesar")
           return
       
       logger.info(f"Tablas encontradas: {len(total_list)}, Nuevas tablas: {len(empty_table)}")
       
       # Verificar si el crawler existe
       logger.info("Paso 2: Verificando existencia del crawler...")
       crawler_exists = get_crawler(data_catalog_crawler_name)
       
       if crawler_exists:
           logger.info("El crawler ya existe")
           
           # Si hay nuevas tablas, actualizar el crawler
           if len(empty_table) > 0:
               logger.info(f"Actualizando crawler con {len(empty_table)} nuevas tablas...")
               if edit_crawler(total_list):
                   logger.info("Crawler actualizado exitosamente")
               else:
                   logger.error("Error actualizando el crawler")
                   return
           
           logger.info("Paso 3: Iniciando crawler existente...")
           if start_crawler(data_catalog_crawler_name):
               logger.info("Crawler iniciado exitosamente")
           else:
               logger.error("Error iniciando el crawler")
               return
               
       else:
           logger.info("El crawler no existe, procediendo a crearlo...")
           
           # Paso 3: Crear la base de datos (si no existe)
           logger.info("Paso 3: Creando base de datos del catálogo (si no existe)...")
           try:
               create_database_data_catalog(data_catalog_database_name)
               logger.info("Base de datos creada o ya existente")
           except Exception as e:
               logger.error(f"Error creando/verificando la base de datos: {str(e)}")
               return

           # Paso 4: Otorgar permisos de Lake Formation a la base de datos
           job_role_arn_name = arn_role_crawler
           logger.info("Otorgando permisos de Lake Formation a la base de datos...")
           try:
               grant_permissions_to_database_lakeformation(job_role_arn_name, data_catalog_database_name)
           except Exception as e:
               logger.warning(f"Error otorgando permisos de base de datos (continuando): {str(e)}")

           # Paso 5: Verificar y asignar LF-Tags si es necesario
           logger.info("Paso 5: Verificando LF-Tags en la base de datos...")
           if not check_lf_tags_on_database(data_catalog_database_name):
               logger.info("La base de datos no tiene LF-Tags, agregándolos...")
               # Primero asegurar que el LF-Tag existe
               try:
                   create_lf_tag_if_not_exists('Level', ['Stage'])
               except Exception as e:
                   logger.warning(f"Error creando LF-Tag (continuando): {str(e)}")
               # Asegurar permisos de LF-Tag para el rol
               try:
                   grant_permissions_lf_tag_lakeformation(job_role_arn_name)
               except Exception as e:
                   logger.warning(f"Error otorgando permisos LF-Tag (continuando): {str(e)}")
               # Agregar LF-Tags a la base de datos
               try:
                   add_lf_tags_to_database_lakeformation(data_catalog_database_name)
               except Exception as e:
                   logger.warning(f"Error agregando LF-Tags (continuando): {str(e)}")
           else:
               logger.info("La base de datos ya tiene los LF-Tags requeridos")

           # Paso 6: Crear el crawler
           logger.info("Paso 6: Creando nuevo crawler...")
           if create_crawler(total_list):
               logger.info("Crawler creado exitosamente")
               logger.info("Paso 7: Iniciando nuevo crawler...")
               if start_crawler(data_catalog_crawler_name):
                   logger.info("Crawler iniciado exitosamente")
               else:
                   logger.error("Error iniciando el nuevo crawler")
                   return
           else:
               logger.error("Error creando el crawler")
               return
       
       # Calcular tiempo de ejecución
       execution_time = time.time() - start_time
       logger.info("="*60)
       logger.info("PROCESO COMPLETADO EXITOSAMENTE")
       logger.info(f"Tiempo de ejecución: {execution_time:.2f} segundos")
       logger.info(f"Crawler: {data_catalog_crawler_name}")
       logger.info(f"Base de datos: {data_catalog_database_name}")
       logger.info(f"Tablas procesadas: {len(total_list)}")
       logger.info(f"Nuevas tablas agregadas: {len(empty_table)}")
       logger.info("="*60)
       
   except CrawlerStageError as e:
       logger.error(f"Error específico del CrawlerStage: {str(e)}")
       logger.error("El proceso se detuvo debido a un error controlado")
       sys.exit(1)
       
   except ClientError as e:
       error_code = e.response['Error']['Code']
       error_message = e.response['Error']['Message']
       logger.error(f"Error de cliente AWS: {error_code} - {error_message}")
       logger.error("El proceso se detuvo debido a un error de AWS")
       sys.exit(1)
       
   except Exception as e:
       execution_time = time.time() - start_time
       logger.critical(f"Error crítico no manejado: {str(e)}")
       logger.critical(f"Traceback completo: {traceback.format_exc()}")
       logger.critical(f"Tiempo transcurrido antes del error: {execution_time:.2f} segundos")
       logger.critical("="*60)
       logger.critical("PROCESO TERMINADO CON ERROR CRÍTICO")
       logger.critical("="*60)
       sys.exit(1)

def health_check() -> bool:
   """Realiza verificaciones de salud del sistema antes de ejecutar el proceso principal"""
   logger.info("Ejecutando verificaciones de salud...")
   
   health_checks = []
   
   try:
       # 1. Verificar conectividad de Glue
       logger.debug("Verificando conectividad con AWS Glue...")
       try:
           # Usar get_databases en lugar de list_databases
           glue_response = client_glue.get_databases(MaxResults=1)
           health_checks.append(("AWS Glue", True, "Conectividad OK"))
           logger.debug("AWS Glue - Conectividad verificada")
       except Exception as e:
           health_checks.append(("AWS Glue", False, f"Error: {str(e)}"))
           logger.error(f"AWS Glue - Error de conectividad: {str(e)}")
       
       # 2. Verificar conectividad de Lake Formation
       logger.debug("Verificando conectividad con AWS Lake Formation...")
       try:
           # En lugar de hacer una llamada específica, verificamos que el cliente se inicialice correctamente
           # y que tengamos permisos básicos
           lf_response = client_lakeformation.list_permissions(MaxResults=1)
           health_checks.append(("AWS Lake Formation", True, "Conectividad OK"))
           logger.debug("AWS Lake Formation - Conectividad verificada")
       except ClientError as e:
           error_code = e.response['Error']['Code']
           # Algunos errores son esperados dependiendo de los permisos
           if error_code in ['AccessDeniedException']:
               health_checks.append(("AWS Lake Formation", True, "Cliente inicializado (permisos limitados)"))
               logger.debug("AWS Lake Formation - Cliente funcional con permisos limitados")
           else:
               health_checks.append(("AWS Lake Formation", False, f"Error: {error_code}"))
               logger.error(f"AWS Lake Formation - Error: {error_code}")
       except Exception as e:
           health_checks.append(("AWS Lake Formation", False, f"Error: {str(e)}"))
           logger.error(f"AWS Lake Formation - Error de conectividad: {str(e)}")
       
       # 3. Verificar acceso a tabla de logs DynamoDB
       logger.debug("Verificando acceso a tabla de logs DynamoDB...")
       try:
           # Usar el cliente DynamoDB para describe_table
           logs_response = dynamodb_client.describe_table(TableName=dynamo_logs_table)
           table_status = logs_response['Table']['TableStatus']
           if table_status == 'ACTIVE':
               health_checks.append(("DynamoDB Logs Table", True, f"Estado: {table_status}"))
               logger.debug(f"Tabla de logs DynamoDB - Estado: {table_status}")
           else:
               health_checks.append(("DynamoDB Logs Table", False, f"Estado no activo: {table_status}"))
               logger.warning(f"Tabla de logs DynamoDB - Estado: {table_status}")
       except Exception as e:
           health_checks.append(("DynamoDB Logs Table", False, f"Error: {str(e)}"))
           logger.error(f"Tabla de logs DynamoDB - Error: {str(e)}")
       
       # 4. Verificar permisos del rol
       logger.debug("Verificando permisos del rol...")
       try:
           # Test básico de permisos intentando describir el rol
           sts_client = boto3.client('sts')
           identity = sts_client.get_caller_identity()
           health_checks.append(("IAM Permissions", True, f"Principal: {identity.get('Arn', 'Unknown')}"))
           logger.debug(f"Permisos IAM - Principal verificado: {identity.get('Arn', 'Unknown')}")
       except Exception as e:
           health_checks.append(("IAM Permissions", False, f"Error: {str(e)}"))
           logger.error(f"Permisos IAM - Error: {str(e)}")
       
       # 5. Verificar acceso a S3
       logger.debug("Verificando acceso a S3...")
       try:
           s3_client = boto3.client('s3')
           # Extraer bucket name del S3 target
           bucket_name = s3_target
           s3_client.head_bucket(Bucket=bucket_name)
           health_checks.append(("S3 Access", True, f"Bucket accesible: {bucket_name}"))
           logger.debug(f"S3 - Bucket accesible: {bucket_name}")
       except Exception as e:
           health_checks.append(("S3 Access", False, f"Error: {str(e)}"))
           logger.error(f"S3 - Error de acceso: {str(e)}")
       
       # 6. Verificar configuración del endpoint
       logger.debug("Verificando datos del endpoint...")
       try:
           if crawler_config and 'endpoint_name' in crawler_config:
               health_checks.append(("Endpoint Config", True, f"Endpoint '{endpoint_name}' encontrado en configuración"))
               logger.debug(f"Datos del endpoint - Endpoint '{endpoint_name}' en configuración")
           else:
               health_checks.append(("Endpoint Config", False, "Configuración de endpoint incompleta"))
               logger.error(f"Datos del endpoint - Configuración incompleta")
       except Exception as e:
           health_checks.append(("Endpoint Config", False, f"Error: {str(e)}"))
           logger.error(f"Datos del endpoint - Error: {str(e)}")
       
       # Resumen de verificaciones de salud
       logger.info("Resumen de verificaciones de salud:")
       logger.info("-" * 60)
       
       failed_checks = 0
       critical_failures = 0
       
       for service, status, message in health_checks:
           status_symbol = "OK" if status else "ERROR"
           logger.info(f"[{status_symbol}] {service:<25} - {message}")
           if not status:
               failed_checks += 1
               # Marcar fallas críticas que impedirían la ejecución
               if service in ["AWS Glue", "DynamoDB Config Table", "DynamoDB Endpoint Table", "Endpoint Data"]:
                   critical_failures += 1
       
       logger.info("-" * 60)
       
       if failed_checks == 0:
           logger.info("Todas las verificaciones de salud pasaron exitosamente")
           return True
       elif critical_failures == 0:
           logger.warning(f"{failed_checks} verificaciones fallaron, pero ninguna es crítica")
           logger.warning("Continuando con el proceso...")
           return True
       else:
           logger.error(f"{critical_failures} verificaciones críticas fallaron de {failed_checks} totales")
           logger.error("No se puede continuar con el proceso")
           return False
           
   except Exception as e:
       logger.error(f"Error durante las verificaciones de salud: {str(e)}")
       logger.error(f"Traceback: {traceback.format_exc()}")
       return False

# Función de monitoreo del progreso del crawler
@retry_on_failure()
def monitor_crawler_progress(crawler_name: str, timeout_minutes: int = 30) -> bool:
   """Monitorea el progreso del crawler hasta su finalización"""
   logger.info(f"Iniciando monitoreo del crawler: {crawler_name}")
   logger.info(f"Timeout configurado: {timeout_minutes} minutos")
   
   start_time = time.time()
   timeout_seconds = timeout_minutes * 60
   check_interval = 30  # Verificar cada 30 segundos
   
   try:
       while True:
           elapsed_time = time.time() - start_time
           
           # Verificar timeout
           if elapsed_time > timeout_seconds:
               logger.warning(f"Timeout alcanzado ({timeout_minutes} minutos) - Deteniendo monitoreo")
               return False
           
           # Obtener estado del crawler
           response = client_glue.get_crawler(Name=crawler_name)
           crawler_state = response['Crawler']['State']
           
           # Log del estado actual
           minutes_elapsed = elapsed_time / 60
           logger.info(f"Estado del crawler [{minutes_elapsed:.1f}min]: {crawler_state}")
           
           # Verificar estados finales
           if crawler_state == 'READY':
               # Obtener estadísticas de la última ejecución
               last_crawl = response['Crawler'].get('LastCrawl', {})
               if last_crawl:
                   status = last_crawl.get('Status', 'UNKNOWN')
                   tables_created = last_crawl.get('TablesCreated', 0)
                   tables_updated = last_crawl.get('TablesUpdated', 0)
                   tables_deleted = last_crawl.get('TablesDeleted', 0)
                   
                   logger.info(f"Crawler completado exitosamente:")
                   logger.info(f"  - Estado final: {status}")
                   logger.info(f"  - Tablas creadas: {tables_created}")
                   logger.info(f"  - Tablas actualizadas: {tables_updated}")
                   logger.info(f"  - Tablas eliminadas: {tables_deleted}")
                   logger.info(f"  - Tiempo total: {minutes_elapsed:.1f} minutos")
               
               return True
               
           elif crawler_state in ['STOPPING', 'STOPPED']:
               logger.warning(f"Crawler detenido inesperadamente - Estado: {crawler_state}")
               
               # Intentar obtener información del error si está disponible
               last_crawl = response['Crawler'].get('LastCrawl', {})
               if last_crawl and 'ErrorMessage' in last_crawl:
                   logger.error(f"Error del crawler: {last_crawl['ErrorMessage']}")
               
               return False
               
           elif crawler_state == 'RUNNING':
               # Obtener progreso si está disponible
               last_crawl = response['Crawler'].get('LastCrawl', {})
               if last_crawl and 'TablesCreated' in last_crawl:
                   logger.debug(f"Progreso: {last_crawl.get('TablesCreated', 0)} tablas procesadas")
           
           # Esperar antes de la siguiente verificación
           time.sleep(check_interval)
           
   except ClientError as e:
       error_code = e.response['Error']['Code']
       logger.error(f"Error monitoreando crawler: {error_code} - {str(e)}")
       raise
   except Exception as e:
       logger.error(f"Error inesperado monitoreando crawler: {str(e)}")
       raise

# Función para limpiar recursos en caso de error
def cleanup_on_error(crawler_name: str, database_name: str):
   """Limpia recursos parcialmente creados en caso de error"""
   logger.info("Iniciando limpieza de recursos debido a error...")
   
   try:
       # Intentar detener el crawler si está corriendo
       try:
           crawler_response = client_glue.get_crawler(Name=crawler_name)
           if crawler_response['Crawler']['State'] == 'RUNNING':
               logger.info(f"Deteniendo crawler en ejecución: {crawler_name}")
               client_glue.stop_crawler(Name=crawler_name)
               
               # Esperar a que se detenga
               max_wait = 60  # 1 minuto
               wait_time = 0
               while wait_time < max_wait:
                   time.sleep(5)
                   wait_time += 5
                   state_response = client_glue.get_crawler(Name=crawler_name)
                   if state_response['Crawler']['State'] != 'RUNNING':
                       logger.info("Crawler detenido exitosamente")
                       break
       except ClientError as e:
           if e.response['Error']['Code'] != 'EntityNotFoundException':
               logger.warning(f"Error deteniendo crawler: {str(e)}")
       
       logger.info("Limpieza completada")
       
   except Exception as e:
       logger.error(f"Error durante la limpieza: {str(e)}")

# Punto de entrada principal
if __name__ == "__main__":
   try:
       # Ejecutar verificaciones de salud primero
       logger.info("Iniciando verificaciones de salud del sistema...")
       if not health_check():
           logger.error("Las verificaciones de salud fallaron, abortando proceso")
           sys.exit(1)
       
       # Ejecutar proceso principal
       main()
       
   except KeyboardInterrupt:
       logger.warning("Proceso interrumpido por el usuario")
       cleanup_on_error(data_catalog_crawler_name, data_catalog_database_name)
       sys.exit(1)
   except Exception as e:
       logger.critical(f"Error crítico no capturado: {str(e)}")
       logger.critical(f"Traceback: {traceback.format_exc()}")
       cleanup_on_error(data_catalog_crawler_name, data_catalog_database_name)
       sys.exit(1)