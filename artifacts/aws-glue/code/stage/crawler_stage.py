import datetime as dt
import logging
import os
import sys
import boto3
import pytz
from awsglue.utils import getResolvedOptions

# Obtener parámetros del trabajo
args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'S3_STAGE_PREFIX', 'DYNAMO_CONFIG_TABLE', 'DYNAMO_ENDPOINT_TABLE', 
                'INPUT_ENDPOINT', 'PROCESS_ID', 'ARN_ROLE_CRAWLER', 'PROJECT_NAME', 'TEAM', 'DATA_SOURCE'])

# Configuración del logger
logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(args['JOB_NAME'])
logger.setLevel(os.environ.get("LOGGING", logging.DEBUG))

# Configuración de zona horaria
TZ_LIMA = pytz.timezone('America/Lima')
YEARS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%Y')
MONTHS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%m')
DAYS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%d')

# Clase utilitaria para gestionar recursos de AWS Glue
class GlueCrawlerManager:
    """Clase utilitaria para gestionar recursos de AWS Glue"""
    
    def __init__(self, logger=None):
        """Inicializar la clase con servicios AWS"""
        self.logger = logger or logging.getLogger(__name__)
        
        try:
            self.dynamodb = boto3.resource('dynamodb')
            self.client_glue = boto3.client('glue')
            self.client_lakeformation = boto3.client('lakeformation')
        except Exception as e:
            self.logger.error(f"Error al inicializar servicios AWS: {str(e)}")
            raise

    def create_database(self, database_name):
        """Crear base de datos en el catálogo de datos"""
        try:
            self.client_glue.create_database(
                DatabaseInput={
                    'Name': database_name
                }
            )
            self.logger.debug(f"Base de datos {database_name} creada exitosamente")
            return True
        except Exception as e:
            self.logger.error(f"Error al crear la base de datos {database_name}: {str(e)}")
            return False

    def database_exists(self, database_name):
        """Verificar si existe la base de datos en el catálogo"""
        try:
            self.client_glue.get_database(
                Name=database_name
            )
            self.logger.debug(f"Base de datos {database_name} encontrada")
            return True
        except self.client_glue.exceptions.EntityNotFoundException:
            self.logger.debug(f"Base de datos {database_name} no encontrada")
            return False
        except Exception as e:
            self.logger.error(f"Error al verificar la base de datos {database_name}: {str(e)}")
            return False

    def get_job_role_arn(self, job_name):
        """Obtener el ARN del rol del trabajo"""
        try:
            role = self.client_glue.get_job(JobName=job_name)['Job']['Role']
            self.logger.debug(f"Rol ARN obtenido: {role}")
            return role
        except Exception as e:
            self.logger.error(f"Error al obtener el rol ARN del trabajo {job_name}: {str(e)}")
            return None

    def grant_database_permissions(self, role_arn, database_name):
        """Asignar permisos de la base de datos en Lake Formation"""
        try:
            self.client_lakeformation.grant_permissions(
                Principal={
                    'DataLakePrincipalIdentifier': role_arn
                },
                Resource={
                    'Database': {
                        'Name': database_name
                    },
                },
                Permissions=[
                    'ALL',
                ],
                PermissionsWithGrantOption=[
                    'ALL',
                ]
            )
            self.logger.debug(f"Permisos concedidos a {role_arn} sobre {database_name}")
            return True
        except Exception as e:
            self.logger.error(f"Error al conceder permisos en Lake Formation: {str(e)}")
            return False

    def grant_lf_tag_permissions(self, role_arn, tag_key="Level", tag_values=["Stage"]):
        """Asignar permisos de etiquetas LF al rol"""
        try:
            self.client_lakeformation.grant_permissions(
                Principal={
                    'DataLakePrincipalIdentifier': role_arn
                },
                Resource={
                    'LFTag': {
                        'TagKey': tag_key,
                        'TagValues': tag_values
                    },
                },
                Permissions=[
                    'ASSOCIATE',
                ],
                PermissionsWithGrantOption=[
                    'ASSOCIATE',
                ]
            )
            self.logger.debug(f"Permisos LF-Tag concedidos a {role_arn}")
            return True
        except Exception as e:
            self.logger.error(f"Error al conceder permisos LF-Tag: {str(e)}")
            return False

    def add_lf_tags_to_database(self, database_name, tag_key="Level", tag_values=["Stage"]):
        """Añadir etiquetas LF a la base de datos"""
        try:
            self.client_lakeformation.add_lf_tags_to_resource(
                Resource={
                    'Database': {
                        'Name': database_name
                    },
                },
                LFTags=[
                    {
                        'TagKey': tag_key,
                        'TagValues': tag_values
                    },
                ]
            )
            self.logger.debug(f"Etiquetas LF añadidas a {database_name}")
            return True
        except Exception as e:
            self.logger.error(f"Error al añadir etiquetas LF a la base de datos: {str(e)}")
            return False

    def create_delta_crawler(self, crawler_name, role_arn, database_name, delta_targets):
        """Crear crawler para objetivos Delta"""
        try:
            if not delta_targets:
                self.logger.warning("No hay objetivos para crear el crawler")
                return False
                
            self.client_glue.create_crawler(
                Name=crawler_name,
                Role=role_arn,
                DatabaseName=database_name,
                Targets={
                    'DeltaTargets': delta_targets
                }
            )
            self.logger.debug(f"Crawler {crawler_name} creado exitosamente")
            return True
        except Exception as e:
            self.logger.error(f"Error al crear crawler {crawler_name}: {str(e)}")
            return False

    def update_delta_crawler(self, crawler_name, role_arn, database_name, delta_targets):
        """Actualizar crawler existente"""
        try:
            if not delta_targets:
                self.logger.warning("No hay objetivos para actualizar el crawler")
                return False
                
            self.client_glue.update_crawler(
                Name=crawler_name,
                Role=role_arn,
                DatabaseName=database_name,
                Targets={
                    'DeltaTargets': delta_targets
                }
            )
            self.logger.debug(f"Crawler {crawler_name} actualizado exitosamente")
            return True
        except Exception as e:
            self.logger.error(f"Error al actualizar crawler {crawler_name}: {str(e)}")
            return False

    def crawler_exists(self, crawler_name):
        """Verificar si existe el crawler"""
        try:
            self.client_glue.get_crawler(
                Name=crawler_name
            )
            self.logger.debug(f"Crawler {crawler_name} encontrado")
            return True
        except self.client_glue.exceptions.EntityNotFoundException:
            self.logger.debug(f"Crawler {crawler_name} no encontrado")
            return False
        except Exception as e:
            self.logger.error(f"Error al verificar crawler {crawler_name}: {str(e)}")
            return False

    def start_crawler(self, crawler_name):
        """Iniciar el crawler"""
        try:
            # Verificar si el crawler ya está en ejecución
            crawler_info = self.client_glue.get_crawler(Name=crawler_name)
            crawler_state = crawler_info.get('Crawler', {}).get('State')
            
            if crawler_state == 'RUNNING':
                self.logger.info(f"El crawler {crawler_name} ya está en ejecución")
                return True
                
            self.client_glue.start_crawler(
                Name=crawler_name
            )
            self.logger.debug(f"Crawler {crawler_name} iniciado exitosamente")
            return True
        except Exception as e:
            self.logger.error(f"Error al iniciar crawler {crawler_name}: {str(e)}")
            return False

    def update_dynamodb_attribute(self, table_name, key_name, key_value, attr_name, attr_value):
        """Actualizar atributo en DynamoDB"""
        try:
            self.logger.info(f'Actualizando DynamoDB: {key_name}={key_value}, {attr_name}={attr_value}, tabla={table_name}')
            dynamo_table = self.dynamodb.Table(table_name)
            response = dynamo_table.update_item(
                Key={key_name: key_value},
                AttributeUpdates={
                    attr_name: {
                        'Value': attr_value,
                        'Action': 'PUT'
                    }
                }
            )
            return True
        except Exception as e:
            self.logger.error(f"Error al actualizar DynamoDB: {str(e)}")
            return False

    def get_dynamodb_items(self, table_name):
        """Obtener todos los items de una tabla DynamoDB"""
        try:
            dynamo_table = self.dynamodb.Table(table_name)
            return dynamo_table.scan()['Items']
        except Exception as e:
            self.logger.error(f"Error al obtener items de DynamoDB: {str(e)}")
            return []

    def get_dynamodb_item(self, table_name, key_name, key_value):
        """Obtener un item específico de DynamoDB"""
        try:
            dynamo_table = self.dynamodb.Table(table_name)
            response = dynamo_table.get_item(Key={key_name: key_value})
            return response.get('Item')
        except Exception as e:
            self.logger.error(f"Error al obtener item de DynamoDB: {str(e)}")
            return None

# Funciones del flujo de trabajo personalizado
def get_tables_for_endpoint(glue_crawler_manager, config_table, endpoint_name):
    """Obtener las tablas asociadas a un endpoint específico"""
    total_list = []
    empty_table = []
    
    try:
        items = glue_crawler_manager.get_dynamodb_items(config_table)
        
        for stage_output in items:
            try:
                if 'ENDPOINT' in stage_output and stage_output['ENDPOINT'] == endpoint_name:
                    if 'CRAWLER' not in stage_output or not stage_output['CRAWLER']:
                        empty_table.append(stage_output['TARGET_TABLE_NAME'])
                    total_list.append(stage_output['TARGET_TABLE_NAME'])
            except Exception as e:
                if 'TARGET_TABLE_NAME' in stage_output:
                    logger.error(f"Problemas con la tabla {stage_output['TARGET_TABLE_NAME']}: {str(e)}")
                else:
                    logger.error(f"Problemas con un registro de DynamoDB: {str(e)}")

        # Actualizar estado del crawler para tablas vacías
        for table in empty_table:
            glue_crawler_manager.update_dynamodb_attribute(
                config_table, 'TARGET_TABLE_NAME', table, 'CRAWLER', True
            )
            logger.debug(f"Tabla {table} añadida al crawler")

    except Exception as e:
        logger.error(f"Error al obtener tablas para el endpoint {endpoint_name}: {str(e)}")
    
    return total_list, empty_table

def prepare_delta_targets(glue_crawler_manager, config_table, endpoint_table, table_list, s3_target, team, data_source):
    """Preparar los objetivos Delta para el crawler"""
    delta_targets = []
    
    for table in table_list:
        try:
            table_data = glue_crawler_manager.get_dynamodb_item(config_table, 'TARGET_TABLE_NAME', table)
            if not table_data:
                logger.warning(f"No se encontraron datos para la tabla {table}")
                continue
                
            table_endpoint = table_data.get('ENDPOINT')
            if not table_endpoint:
                logger.warning(f"La tabla {table} no tiene ENDPOINT definido")
                continue
                
            data_source = {
                'DeltaTables': [f"{s3_target}{team}/{data_source}/{table_data['ENDPOINT']}/{table_data['STAGE_TABLE_NAME']}/"],
                'ConnectionName': '',
                'CreateNativeDeltaTable': True
            }
            delta_targets.append(data_source)
        except Exception as e:
            logger.error(f"Error al procesar la tabla {table}: {str(e)}")
    
    return delta_targets

def main():
    """Función principal"""
    try:        
        # Inicializar el gestor de Glue
        glue_crawler_manager = GlueCrawlerManager(logger)
        
        # Variables principales
        dynamo_config_table = args['DYNAMO_CONFIG_TABLE']
        dynamo_endpoint_table = args['DYNAMO_ENDPOINT_TABLE']
        s3_target = args['S3_STAGE_PREFIX']
        arn_role_crawler = args['ARN_ROLE_CRAWLER']
        job_name = args['JOB_NAME']
        endpoint_name = args['INPUT_ENDPOINT']
        team = args['TEAM']
        data_source = args['DATA_SOURCE']
        
        # Obtener datos del endpoint
        endpoint_data = glue_crawler_manager.get_dynamodb_item(dynamo_endpoint_table, 'ENDPOINT_NAME', endpoint_name)
        if not endpoint_data:
            logger.error(f"No se encontraron datos para el endpoint {endpoint_name}")
            return
        
        # Definir nombres de recursos
        data_catalog_database_name = f"{team}_{data_source}_{endpoint_name}_stage".lower()
        data_catalog_crawler_name = data_catalog_database_name + "_crawler"
        
        logger.info(f"Procesando endpoint: {endpoint_name}")
        logger.info(f"Base de datos: {data_catalog_database_name}")
        logger.info(f"Crawler: {data_catalog_crawler_name}")
        
        # Obtener tablas para el endpoint
        total_list, empty_table = get_tables_for_endpoint(
            glue_crawler_manager, dynamo_config_table, endpoint_name
        )
        
        if not total_list:
            logger.warning(f"No se encontraron tablas para el endpoint {endpoint_name}")
            return
            
        logger.info(f"Total de tablas: {len(total_list)}, Tablas sin crawler: {len(empty_table)}")
        
        # Preparar objetivos Delta
        delta_targets = prepare_delta_targets(
            glue_crawler_manager, dynamo_config_table, dynamo_endpoint_table, 
            total_list, s3_target, team, data_source
        )
        
        if not delta_targets:
            logger.error("No se pudieron preparar objetivos Delta para el crawler")
            return
        
        # Comprobar si existe el crawler
        if glue_crawler_manager.crawler_exists(data_catalog_crawler_name):
            if empty_table:
                logger.info(f"Actualizando crawler con {len(empty_table)} nuevas tablas")
                glue_crawler_manager.update_delta_crawler(
                    data_catalog_crawler_name, arn_role_crawler, 
                    data_catalog_database_name, delta_targets
                )
            
            logger.info(f"Iniciando crawler existente {data_catalog_crawler_name}")
            glue_crawler_manager.start_crawler(data_catalog_crawler_name)
        else:
            logger.info("Verificando existencia de la base de datos en el catálogo")
            
            if glue_crawler_manager.database_exists(data_catalog_database_name):
                logger.info(f"Creando nuevo crawler para base de datos existente {data_catalog_database_name}")
                glue_crawler_manager.create_delta_crawler(
                    data_catalog_crawler_name, arn_role_crawler, 
                    data_catalog_database_name, delta_targets
                )
                glue_crawler_manager.start_crawler(data_catalog_crawler_name)
            else:
                logger.info("Obteniendo el ARN del rol del trabajo")
                job_role_arn = glue_crawler_manager.get_job_role_arn(job_name)
                
                if not job_role_arn:
                    logger.error(f"No se pudo obtener el ARN del rol para el trabajo {job_name}")
                    return
                
                logger.info("Asignando permisos de etiquetas LF")
                glue_crawler_manager.grant_lf_tag_permissions(job_role_arn)
                
                logger.info(f"Creando base de datos {data_catalog_database_name}")
                if not glue_crawler_manager.create_database(data_catalog_database_name):
                    logger.error("No se pudo crear la base de datos. Abortando.")
                    return
                
                logger.info("Añadiendo etiquetas LF a la base de datos")
                glue_crawler_manager.add_lf_tags_to_database(data_catalog_database_name)
                
                logger.info("Asignando permisos a la base de datos")
                glue_crawler_manager.grant_database_permissions(job_role_arn, data_catalog_database_name)
                
                logger.info("Creando nuevo crawler")
                if glue_crawler_manager.create_delta_crawler(
                    data_catalog_crawler_name, arn_role_crawler, 
                    data_catalog_database_name, delta_targets
                ):
                    logger.info("Iniciando nuevo crawler")
                    glue_crawler_manager.start_crawler(data_catalog_crawler_name)
                else:
                    logger.error("No se pudo crear el crawler. Abortando.")
                    
        logger.info("Proceso completado exitosamente")
    except Exception as e:
        logger.error(f"Error en la función principal: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()