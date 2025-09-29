import datetime as dt
import logging
import os
import sys
import time
import json
import csv
from io import StringIO
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from abc import ABC, abstractmethod
import re

import boto3
import pytz
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from dateutil.relativedelta import relativedelta
from py4j.protocol import Py4JJavaError
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
# Agregar estos imports después de los imports existentes
 
# Constantes
TZ_LIMA = pytz.timezone('America/Lima')
BASE_DATE_MAGIC = "1900-01-01"
MAGIC_OFFSET = 693596

class Monitor:
    """
    Monitor para Light Transform siguiendo el patrón de extract_data_v2
    Centraliza todas las llamadas de logging a DynamoDB para evitar duplicados
    """
    
    def __init__(self, dynamo_logger):
        """
        Inicializa el monitor con un DynamoDBLogger
        
        Args:
            dynamo_logger: Instancia de DynamoDBLogger para registrar eventos
        """
        self.dynamo_logger = dynamo_logger
        self.process_id: Optional[str] = None
    
    def log_start(self, table_name: str, job_name: str, context: Dict[str, Any] = None) -> str:
        """
        Registra el inicio de la transformación - ÚNICA LLAMADA
        
        Args:
            table_name: Nombre de la tabla a transformar
            job_name: Nombre del job de Glue
            context: Contexto adicional para el log
            
        Returns:
            process_id: ID único del proceso
        """
        self.process_id = self.dynamo_logger.log_start(
            table_name=table_name,
            job_name=job_name,
            context=context or {}
        )
        return self.process_id
    
    def log_success(self, table_name: str, job_name: str, context: Dict[str, Any] = None) -> str:
        """
        Registra el éxito de la transformación - ÚNICA LLAMADA
        
        Args:
            table_name: Nombre de la tabla transformada
            job_name: Nombre del job de Glue
            context: Contexto adicional para el log
            
        Returns:
            process_id: ID único del proceso
        """
        return self.dynamo_logger.log_success(
            table_name=table_name,
            job_name=job_name,
            context=context or {}
        )
    
    def log_error(self, table_name: str, error_message: str, job_name: str, context: Dict[str, Any] = None) -> str:
        """
        Registra un error en la transformación - ÚNICA LLAMADA
        
        Args:
            table_name: Nombre de la tabla donde ocurrió el error
            error_message: Mensaje de error
            job_name: Nombre del job de Glue
            context: Contexto adicional para el log
            
        Returns:
            process_id: ID único del proceso
        """
        return self.dynamo_logger.log_failure(
            table_name=table_name,
            error_message=error_message,
            job_name=job_name,
            context=context or {}
        )
    
    def log_warning(self, table_name: str, warning_message: str, job_name: str, context: Dict[str, Any] = None) -> str:
        """
        Registra una advertencia en la transformación
        
        Args:
            table_name: Nombre de la tabla
            warning_message: Mensaje de advertencia
            job_name: Nombre del job de Glue
            context: Contexto adicional para el log
            
        Returns:
            process_id: ID único del proceso
        """
        return self.dynamo_logger.log_warning(
            table_name=table_name,
            warning_message=warning_message,
            job_name=job_name,
            context=context or {}
        )
    
    def get_process_id(self) -> Optional[str]:
        """Retorna el process_id actual"""
        return self.process_id
    
class DataLakeLogger:
    """
    Clase centralizada para logging en DataLake que maneja automáticamente:
    - AWS CloudWatch (en entorno Glue)
    - Console output
    - Detección automática del entorno (AWS vs Local)
    """
    
    # Configuración global por defecto
    _global_config = {
        'log_level': logging.INFO,
        'service_name': 'light_transform',
        'correlation_id': None,
        'owner': None,
        'auto_detect_env': True,
        'force_local_mode': False
    }
    
    # Cache de loggers para evitar recrear
    _logger_cache = {}
    
    @classmethod
    def configure_global(cls, 
                        log_level: Optional[int] = None,
                        service_name: Optional[str] = None,
                        correlation_id: Optional[str] = None,
                        owner: Optional[str] = None,
                        auto_detect_env: bool = True,
                        force_local_mode: bool = False):
        """Configura parámetros globales para todos los loggers"""
        if log_level is not None:
            cls._global_config['log_level'] = log_level
        if service_name is not None:
            cls._global_config['service_name'] = service_name
        if correlation_id is not None:
            cls._global_config['correlation_id'] = correlation_id
        if owner is not None:
            cls._global_config['owner'] = owner
        
        cls._global_config['auto_detect_env'] = auto_detect_env
        cls._global_config['force_local_mode'] = force_local_mode
        
        # Limpiar cache cuando cambia configuración
        cls._logger_cache.clear()
    
    @classmethod
    def get_logger(cls, 
                   name: Optional[str] = None,
                   service_name: Optional[str] = None,
                   correlation_id: Optional[str] = None,
                   log_level: Optional[int] = None) -> logging.Logger:
        """Obtiene un logger configurado para el entorno actual"""
        
        # Usar configuración global como base
        effective_service = service_name or cls._global_config['service_name']
        effective_correlation_id = correlation_id or cls._global_config['correlation_id']
        effective_log_level = log_level or cls._global_config['log_level']
        
        # Crear cache key
        cache_key = f"{name}_{effective_service}_{effective_correlation_id}_{effective_log_level}"
        
        # Devolver del cache si existe
        if cache_key in cls._logger_cache:
            return cls._logger_cache[cache_key]
        
        # Crear logger estándar de Python
        logger_name = name or effective_service or 'light_transform'
        logger = logging.getLogger(logger_name)
        logger.setLevel(effective_log_level)
        
        # Limpiar handlers existentes para evitar duplicados
        if logger.handlers:
            logger.handlers.clear()
        
        # Crear formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Handler para consola (CloudWatch en Glue)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(effective_log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # Evitar propagación para prevenir logs duplicados
        logger.propagate = False
        
        # Guardar en cache
        cls._logger_cache[cache_key] = logger
        
        return logger

class DynamoDBLogger:
    """
    Logger para DynamoDB que registra logs de proceso y envía notificaciones SNS en caso de errores
    """
    
    def __init__(
        self,
        table_name: str,
        sns_topic_arn: Optional[str] = None,
        team: str = "",
        data_source: str = "",
        endpoint_name: str = "",
        flow_name: str = "",
        environment: str = "",
        region: str = "us-east-1",
        logger_name: Optional[str] = None
    ):
        """Inicializa el DynamoDB Logger"""
        self.table_name = table_name
        self.sns_topic_arn = sns_topic_arn
        self.team = team
        self.data_source = data_source
        self.endpoint_name = endpoint_name
        self.flow_name = flow_name
        self.environment = environment
        
        # Configurar timezone Lima
        self.tz_lima = pytz.timezone('America/Lima')
        
        # Obtener logger usando DataLakeLogger
        self.logger = DataLakeLogger.get_logger(
            name=logger_name or f"{team}-{data_source}-dynamodb-logger",
            service_name=f"{team}-{flow_name}",
            correlation_id=f"{team}-{data_source}-{flow_name}"
        )
        
        # Clientes AWS
        try:
            self.dynamodb = boto3.resource('dynamodb', region_name=region)
            self.dynamodb_table = self.dynamodb.Table(table_name) if table_name else None
            self.sns_client = boto3.client('sns', region_name=region) if sns_topic_arn else None
            
            self.logger.info(f"DynamoDBLogger inicializado - Tabla: {table_name}, SNS: {bool(sns_topic_arn)}")
            
        except Exception as e:
            self.logger.warning(f"Error inicializando clientes AWS: {e}")
            self.dynamodb_table = None
            self.sns_client = None
    
    def log_process_status(
        self,
        status: str,  # RUNNING, SUCCESS, FAILED, WARNING
        message: str,
        table_name: str = "",
        job_name: str = "",
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Registra el estatus de un proceso en DynamoDB"""
        if not self.dynamodb_table:
            self.logger.warning(f"DynamoDB no configurado, log no registrado: {status} - {message}")
            return ""
        
        try:
            # Generar timestamp y process_id únicos
            now_lima = dt.datetime.now(pytz.utc).astimezone(self.tz_lima)
            timestamp = now_lima.strftime("%Y%m%d_%H%M%S_%f")
            process_id = f"{self.team}-{self.data_source}-{self.flow_name}-{table_name}-{timestamp}"
            
            # Preparar contexto con límites de tamaño
            log_context = self._prepare_context(context or {})
            
            # Truncar mensaje si es muy largo
            truncated_message = message[:2000] + "...[TRUNCATED]" if len(message) > 2000 else message
            
            # Crear registro compatible con estructura existente
            record = {
                "PROCESS_ID": process_id,
                "DATE_SYSTEM": timestamp,
                "RESOURCE_NAME": job_name or "unknown_job",
                "RESOURCE_TYPE": "python_shell_glue_job",
                "STATUS": status.upper(),
                "MESSAGE": truncated_message,
                "PROCESS_TYPE": self._get_process_type(status),
                "CONTEXT": log_context,
                "TEAM": self.team,
                "DATASOURCE": self.data_source,
                "ENDPOINT_NAME": self.endpoint_name,
                "TABLE_NAME": table_name,
                "ENVIRONMENT": self.environment,
                "log_created_at": now_lima.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Insertar en DynamoDB
            self.dynamodb_table.put_item(Item=record)
            self.logger.info(f"Log registrado en DynamoDB", {
                "process_id": process_id, 
                "status": status,
                "table": table_name
            })
            
            # Enviar notificación SNS si es error
            if status.upper() == "FAILED":
                self._send_failure_notification(record)
            
            return process_id
            
        except Exception as e:
            self.logger.error(f"Error registrando log en DynamoDB: {e}")
            
            # Si falló el registro pero era un error, intentar enviar SNS de emergencia
            if status.upper() == "FAILED":
                self._send_emergency_notification(message, table_name, str(e))
            
            return ""
    
    def log_start(
        self, 
        table_name: str, 
        job_name: str = "",
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Registra inicio de proceso"""
        message = f"Iniciando procesamiento de tabla {table_name}"
        self.logger.info(message, {"table": table_name, "job": job_name})
        return self.log_process_status("RUNNING", message, table_name, job_name, context)
    
    def log_success(
        self, 
        table_name: str, 
        job_name: str = "",
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Registra éxito de proceso"""
        message = f"Procesamiento exitoso de tabla {table_name}"
        self.logger.info(message, {"table": table_name, "job": job_name})
        return self.log_process_status("SUCCESS", message, table_name, job_name, context)
    
    def log_failure(
        self, 
        table_name: str, 
        error_message: str,
        job_name: str = "",
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Registra fallo de proceso y envía notificación"""
        message = f"Error procesando tabla {table_name}: {error_message}"
        self.logger.error(message, {"table": table_name, "job": job_name, "error": error_message})
        return self.log_process_status("FAILED", message, table_name, job_name, context)
    
    def log_warning(
        self, 
        table_name: str, 
        warning_message: str,
        job_name: str = "",
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Registra advertencia de proceso"""
        message = f"Advertencia procesando tabla {table_name}: {warning_message}"
        self.logger.warning(message, {"table": table_name, "job": job_name, "warning": warning_message})
        return self.log_process_status("WARNING", message, table_name, job_name, context)
    
    def _prepare_context(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Prepara el contexto limitando su tamaño para DynamoDB"""
        MAX_CONTEXT_SIZE = 300 * 1024  # 300KB
        
        def truncate_data(data, max_length=1000):
            """Trunca estructuras de datos"""
            if isinstance(data, str):
                return data[:max_length] + "...[TRUNCATED]" if len(data) > max_length else data
            elif isinstance(data, dict):
                truncated = {}
                for k, v in list(data.items())[:10]:
                    truncated[k] = truncate_data(v, 500)
                if len(data) > 10:
                    truncated["_truncated_items"] = f"...and {len(data) - 10} more items"
                return truncated
            elif isinstance(data, list):
                truncated = [truncate_data(item, 200) for item in data[:5]]
                if len(data) > 5:
                    truncated.append(f"...and {len(data) - 5} more items")
                return truncated
            else:
                return str(data)[:500] if data else data
        
        prepared_context = truncate_data(context)
        
        # Verificar tamaño total
        context_json = json.dumps(prepared_context, default=str)
        if len(context_json.encode("utf-8")) > MAX_CONTEXT_SIZE:
            return {
                "size_limit_applied": "Context truncated due to DynamoDB size limits",
                "original_keys": list(context.keys())[:10],
                "truncated_at": dt.datetime.now(self.tz_lima).strftime("%Y-%m-%d %H:%M:%S")
            }
        
        return prepared_context
    
    def _get_process_type(self, status: str) -> str:
        """Determina el tipo de proceso basado en el status"""
        if status.upper() in ["RUNNING"]:
            return "incremental"
        elif status.upper() in ["SUCCESS"]:
            return "completed"
        elif status.upper() in ["WARNING"]:
            return "incremental_with_warnings"
        else:
            return "error_handling"
    
    def _send_failure_notification(self, record: Dict[str, Any]):
        """Envía notificación SNS por error"""
        if not self.sns_client or not self.sns_topic_arn:
            self.logger.warning("SNS no configurado, no se puede enviar notificación de error")
            return
        
        try:
            # Preparar mensaje truncado para SNS
            message_text = str(record.get("MESSAGE", ""))
            truncated_message = message_text[:800] + "..." if len(message_text) > 800 else message_text
            
            notification_message = f"""
🚨 PROCESO FALLIDO EN LIGHT TRANSFORM

📊 DETALLES:
- Estado: {record.get('STATUS')}
- Tabla: {record.get("TABLE_NAME")}
- Equipo: {record.get("TEAM")}
- Flujo: {self.flow_name}
- Ambiente: {record.get("ENVIRONMENT")}
- Timestamp: {record.get("log_created_at")}

❌ ERROR:
{truncated_message}

🔍 IDENTIFICADORES:
- Process ID: {record.get('PROCESS_ID')}
- Resource: {record.get('RESOURCE_NAME')}

📋 ACCIONES:
1. Consulta logs completos en DynamoDB usando el PROCESS_ID
2. Revisa CloudWatch logs para más detalles
3. Verifica la configuración de la tabla y transformaciones

⚠️ Este mensaje se envía automáticamente. El job se marca como SUCCESS para evitar dobles notificaciones.
            """
            
            # Enviar notificación
            self.sns_client.publish(
                TopicArn=self.sns_topic_arn,
                Subject=f"🚨 [ERROR] LIGHT TRANSFORM - {record.get('TABLE_NAME')} - {record.get('TEAM')}",
                Message=notification_message
            )
            
            self.logger.info("Notificación SNS enviada exitosamente")
            
        except Exception as e:
            self.logger.error(f"Error enviando notificación SNS: {e}")
    
    def _send_emergency_notification(self, message: str, table_name: str, dynamodb_error: str):
        """Envía notificación de emergencia cuando falla DynamoDB"""
        if not self.sns_client or not self.sns_topic_arn:
            return
        
        try:
            emergency_message = f"""
🆘 NOTIFICACIÓN DE EMERGENCIA - FALLO EN SISTEMA DE LOGGING

⚠️ SITUACIÓN CRÍTICA:
El proceso falló Y el sistema de logging a DynamoDB también falló.

📊 DETALLES DEL ERROR ORIGINAL:
- Tabla: {table_name}
- Equipo: {self.team}
- Flujo: {self.flow_name}
- Error: {message[:500]}

🔧 ERROR DE DYNAMODB:
{dynamodb_error[:300]}

🚨 ACCIÓN REQUERIDA:
1. Revisar logs de CloudWatch INMEDIATAMENTE
2. Verificar conectividad a DynamoDB
3. Revisar permisos IAM
4. Investigar el error original del proceso

⚠️ Sin logging en DynamoDB, la trazabilidad está comprometida.
            """
            
            self.sns_client.publish(
                TopicArn=self.sns_topic_arn,
                Subject=f"🆘 [EMERGENCIA] Sistema de Logging Fallido - {table_name}",
                Message=emergency_message
            )
            
            self.logger.critical("Notificación de emergencia enviada")
            
        except Exception as e:
            self.logger.critical(f"Error crítico: No se pudo enviar notificación de emergencia: {e}")

@dataclass
class ColumnMetadata:
    """Estructura de datos para metadatos de columna"""
    name: str
    column_id: int
    data_type: str
    transformation: str
    is_partition: bool = False
    is_id: bool = False
    is_order_by: bool = False
    is_filter_date: bool = False

@dataclass
class TableConfig:
    """Configuración de tabla"""
    stage_table_name: str
    source_table: str
    source_table_type: str
    load_type: str
    num_days: Optional[str] = None
    delay_incremental_ini: str = "-2"

@dataclass
class EndpointConfig:
    """Configuración de endpoint"""
    endpoint_name: str
    environment: str
    src_db_name: str
    src_server_name: str
    src_db_username: str

class TransformationException(Exception):
    """Excepción específica para errores de transformación"""
    def __init__(self, column_name: str, message: str):
        self.column_name = column_name
        self.message = message
        super().__init__(f"Error en columna {column_name}: {message}")

class DataValidationException(Exception):
    """Excepción para errores de validación de datos"""
    pass

class ConfigurationManager:
    """Maneja la carga y validación de configuraciones desde S3"""
    
    def __init__(self, s3_client):
        self.s3_client = s3_client
    
    def load_csv_from_s3(self, s3_path: str) -> List[Dict[str, str]]:
        """Carga archivo CSV desde S3 con validación"""
        try:
            bucket = s3_path.split('/')[2]
            key = '/'.join(s3_path.split('/')[3:])
            
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('latin1')  # Cambio a UTF-8
            
            csv_data = []
            reader = csv.DictReader(StringIO(content), delimiter=';')
            for row in reader:
                # Sanitizar datos
                sanitized_row = self._sanitize_csv_row(row)
                csv_data.append(sanitized_row)
            
            return csv_data
        except Exception as e:
            raise DataValidationException(f"Error cargando CSV desde {s3_path}: {str(e)}")
    
    def _sanitize_csv_row(self, row: Dict[str, str]) -> Dict[str, str]:
        """Sanitiza una fila de CSV removiendo comillas"""
        sanitized = {}
        for key, value in row.items():
            if isinstance(value, str):
                # Remover comillas triples y dobles
                clean_value = value.replace('"""', '"')
                if clean_value.startswith('"') and clean_value.endswith('"'):
                    clean_value = clean_value[1:-1]
                sanitized[key] = clean_value
            else:
                sanitized[key] = value
        return sanitized

class ExpressionParser:
    """Parser robusto para expresiones de transformación"""
    
    def __init__(self):
        self.function_pattern = re.compile(r'(\w+)\((.*)\)$')
    
    def parse_transformation(self, expression: str) -> List[Tuple[str, List[str]]]:
        """
        Parsea expresión de transformación y retorna lista de (función, parámetros)
        """
        if not expression or expression.strip() == '':
            return []
        
        functions_with_params = []
        remaining = expression.strip()
        
        match = self.function_pattern.match(remaining)
        if not match:
            # No es una función, es una columna simple
            return [('simple_column', [remaining])]
        
        function_name = match.group(1)
        params_str = match.group(2)
        
        # Extraer parámetros
        params = self._extract_parameters(params_str) if params_str else []
        functions_with_params.append((function_name, params))
        
        return functions_with_params
    
    def _extract_parameters(self, params_str: str) -> List[str]:
        """Extrae parámetros de una función manejando comas en strings"""
        if not params_str:
            return []
        
        params = []
        current_param = ""
        paren_count = 0
        in_quotes = False
        
        i = 0
        while i < len(params_str):
            char = params_str[i]
            
            if char == '"' and (i == 0 or params_str[i-1] != '\\'):
                in_quotes = not in_quotes
                current_param += char
            elif char == '(' and not in_quotes:
                paren_count += 1
                current_param += char
            elif char == ')' and not in_quotes:
                paren_count -= 1
                current_param += char
            elif char == ',' and paren_count == 0 and not in_quotes:
                if current_param.strip():
                    params.append(current_param.strip())
                current_param = ""
            else:
                current_param += char
            
            i += 1
        
        # Agregar último parámetro
        if current_param.strip():
            params.append(current_param.strip())
        
        return params

class TransformationEngine:
    """Motor de transformaciones optimizado"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.parser = ExpressionParser()
    
    def apply_transformations(self, df, columns_metadata: List[ColumnMetadata]) -> Tuple[Any, List[str]]:
        """
        Aplica todas las transformaciones de manera optimizada
        Retorna (DataFrame transformado, lista de errores)
        """
        errors = []
        transformation_exprs = []
        
        # Ordenar columnas por column_id
        sorted_columns = sorted(columns_metadata, key=lambda x: x.column_id)
        
        for column_meta in sorted_columns:
            try:
                expr = self._build_transformation_expression(column_meta)
                if expr is not None:
                    transformation_exprs.append(expr.alias(column_meta.name))
                else:
                    # Columna simple sin transformación
                    if column_meta.transformation and column_meta.transformation.strip():
                        # Si hay transformación pero no se pudo parsear, usar la columna original
                        transformation_exprs.append(col(column_meta.transformation).alias(column_meta.name))
                    else:
                        # Sin transformación definida, crear columna null con tipo apropiado
                        spark_type = self._get_spark_type(column_meta.data_type)
                        transformation_exprs.append(lit(None).cast(spark_type).alias(column_meta.name))
            except Exception as e:
                error_msg = f"Error en columna {column_meta.name}: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)
                # Agregar columna con valor null apropiado en caso de error
                spark_type = self._get_spark_type(column_meta.data_type)
                transformation_exprs.append(lit(None).cast(spark_type).alias(column_meta.name))
        
        # Aplicar todas las transformaciones en una sola operación
        if transformation_exprs:
            transformed_df = df.select(*transformation_exprs)
        else:
            transformed_df = df
        
        return transformed_df, errors
    
    def _get_spark_type(self, data_type: str):
        """Convierte string de tipo a tipo Spark"""
        type_mapping = {
            'string': StringType(),
            'int': IntegerType(),
            'integer': IntegerType(),
            'double': DoubleType(),
            'float': DoubleType(),
            'boolean': BooleanType(),
            'timestamp': TimestampType(),
            'date': DateType()
        }
        
        if 'numeric' in data_type.lower():
            return self._parse_decimal_type(data_type)
        
        return type_mapping.get(data_type.lower(), StringType())
    
    def _build_transformation_expression(self, column_meta: ColumnMetadata):
        """Construye expresión de transformación para una columna"""
        functions_with_params = self.parser.parse_transformation(column_meta.transformation)
        
        if not functions_with_params:
            return None
        
        if len(functions_with_params) == 1 and functions_with_params[0][0] == 'simple_column':
            # Es una columna simple
            column_name = functions_with_params[0][1][0] if functions_with_params[0][1] else column_meta.name
            return col(column_name)
        
        # Es una función
        function_name, params = functions_with_params[0]
        return self._create_transformation_expr(function_name, params, column_meta.data_type)
    
    def _create_transformation_expr(self, function_name: str, params: List[str], data_type: str):
        """Crea expresión de transformación para función específica"""
        logger.info(f"Aplicando transformación: {function_name} con parámetros: {params} y tipo: {data_type}")
        param_list = params if params else []
        
        if function_name == 'fn_transform_Concatenate':
            columns_to_concat = [col(p.strip()) for p in param_list]
            return concat_ws("|", *[coalesce(trim(c), lit("")) for c in columns_to_concat])
        
        elif function_name == 'fn_transform_Concatenate_ws':
            if len(param_list) < 2:
                raise TransformationException("fn_transform_Concatenate_ws", "Requiere al menos 2 parámetros")
            separator = param_list[-1]
            columns_to_concat = [col(p.strip()) for p in param_list[:-1]]
            return concat_ws(separator, *[coalesce(trim(c), lit("")) for c in columns_to_concat])
        
        elif function_name == 'fn_transform_Integer':
            if not param_list:
                raise TransformationException("fn_transform_Integer", "Requiere nombre de columna")
            origin_column = param_list[0]
            
            # Versión ultra-simple
            return coalesce(
                col(origin_column).cast(IntegerType()),
                lit(None).cast(IntegerType())
            )

        elif function_name == 'fn_transform_Double':
            if not param_list:
                raise TransformationException("fn_transform_Double", "Requiere nombre de columna")
            origin_column = param_list[0]
            
            # Versión ultra-simple
            return coalesce(
                col(origin_column).cast(DoubleType()),
                lit(None).cast(DoubleType())
            )

        elif function_name == 'fn_transform_Numeric':
            if not param_list:
                raise TransformationException("fn_transform_Numeric", "Requiere nombre de columna")
            origin_column = param_list[0]
            
            decimal_type = self._parse_decimal_type(data_type)
            
            # Versión ultra-simple
            return coalesce(
                col(origin_column).cast(decimal_type),
                lit(None).cast(decimal_type)
            )

        elif function_name == 'fn_transform_Boolean':
            if not param_list:
                raise TransformationException("fn_transform_Boolean", "Requiere nombre de columna")
            origin_column = param_list[0]
            
            # Versión ultra-simple usando coalesce como los demás
            return coalesce(
                col(origin_column).cast(BooleanType()),
                lit(None).cast(BooleanType())
            )
        
        elif function_name == 'fn_transform_ClearString':
            origin_column = param_list[0] if param_list else None
            if not origin_column:
                raise TransformationException("fn_transform_ClearString", "Requiere nombre de columna")
            
            if len(param_list) > 1:
                default = param_list[1]
                # Si el default empieza con $, es un literal
                if default.startswith('$'):
                    default_expr = lit(default[1:])  # Remover el $
                else:
                    default_expr = col(default)
                
                return when(
                    col(origin_column).isNull() | 
                    (trim(col(origin_column)) == "") |
                    (trim(col(origin_column)).isin(["None", "NULL", "null"])),  # MÁS CASOS
                    default_expr
                ).otherwise(trim(col(origin_column)))
            else:
                # Sin valor por defecto - devolver NULL real para valores vacíos/nulos
                return when(
                    col(origin_column).isNull() |
                    (trim(col(origin_column)) == "") |
                    (trim(col(origin_column)).isin(["None", "NULL", "null"])),  # MÁS CASOS
                    lit(None).cast(StringType())
                ).otherwise(
                    trim(col(origin_column))
                )
        
        elif function_name == 'fn_transform_DateMagic':
            if len(param_list) < 3:
                raise TransformationException(function_name, "Requiere 3 parámetros: column, format, default")
            
            origin_column = param_list[0]
            date_format_param = param_list[1]
            value_default = param_list[2]

            date_pattern = r'^([7-9]\d{5}|[1-2]\d{6}|3[0-5]\d{5})$'

            return when(
                regexp_extract(col(origin_column).cast(StringType()), date_pattern, 1) != "",
                to_date(
                    date_add(
                        to_date(lit(BASE_DATE_MAGIC)), 
                        col(origin_column).cast(IntegerType()) - lit(MAGIC_OFFSET)
                    ),
                    date_format_param
                )
            ).otherwise(
                to_date(lit(value_default), date_format_param)
            ).cast(DateType())


        elif function_name == 'fn_transform_DatetimeMagic':
            if len(param_list) < 4:
                raise TransformationException(function_name, "Requiere 4 parámetros: column_date, column_time, format, default")
            
            origin_column_date = param_list[0]
            origin_column_time = param_list[1]
            datetime_format = param_list[2]
            value_default = param_list[3]

            date_pattern = r'^([7-9]\d{5}|[1-2]\d{6}|3[0-5]\d{5})$'
            time_pattern = r'^([01][0-9]|2[0-3])([0-5][0-9])([0-5][0-9])$'

            return when(
                regexp_extract(col(origin_column_date).cast(StringType()), date_pattern, 1) != "",
                when(
                    regexp_extract(col(origin_column_time).cast(StringType()), time_pattern, 1) != "",
                    to_timestamp(
                        concat_ws(" ", 
                            to_date(
                                date_add(
                                    to_date(lit(BASE_DATE_MAGIC)), 
                                    col(origin_column_date).cast(IntegerType()) - lit(MAGIC_OFFSET)
                                )
                            ),
                            concat_ws(
                                ":", 
                                col(origin_column_time).substr(1, 2),
                                col(origin_column_time).substr(3, 2),
                                col(origin_column_time).substr(5, 2)
                            )
                        ),
                        datetime_format
                    )
                ).otherwise(
                    to_timestamp(
                        date_add(
                            to_date(lit(BASE_DATE_MAGIC)), 
                            col(origin_column_date).cast(IntegerType()) - lit(MAGIC_OFFSET)
                        ),
                        datetime_format[:8]  # solo fecha
                    )
                )
            ).otherwise(
                to_timestamp(lit(value_default), datetime_format[:8])
            ).cast(TimestampType())
        
        elif function_name == 'fn_transform_Datetime':
            # Verificar parámetros mínimos
            if len(param_list) < 1:
                # Sin parámetros - usar timestamp actual
                return from_utc_timestamp(current_timestamp(), "America/Lima")
            
            origin_column = param_list[0]
            
            # Si el primer parámetro es vacío o NULL, usar timestamp actual
            if not origin_column or origin_column.upper() in ['NULL', 'NONE', '']:
                return from_utc_timestamp(current_timestamp(), "America/Lima")
            
            # Obtener formato y valor por defecto
            date_format_param = param_list[1] if len(param_list) > 1 else "yyyy-MM-dd HH:mm:ss"
            value_default = param_list[2] if len(param_list) > 2 else None
            
            # Crear valor por defecto
            if value_default and value_default.upper() not in ['NULL', 'NONE', '']:
                try:
                    default_expr = to_timestamp(lit(value_default), date_format_param)
                except:
                    default_expr = lit(None).cast(TimestampType())
            else:
                # Si especifica NULL o no hay default, usar null
                default_expr = lit(None).cast(TimestampType())
            
            # Versión simplificada usando coalesce
            return coalesce(
                # Intentar convertir con el formato especificado
                to_timestamp(col(origin_column), date_format_param),
                # Si falla, usar valor por defecto
                default_expr
            )
        
        elif function_name == 'fn_transform_Date':
            # Verificar parámetros mínimos
            if len(param_list) < 1:
                # Sin parámetros - usar fecha actual
                return current_date()
            
            origin_column = param_list[0]
            
            # Si el primer parámetro es vacío o NULL, usar fecha actual
            if not origin_column or origin_column.upper() in ['NULL', 'NONE', '']:
                return current_date()
            
            # Obtener formato y valor por defecto
            date_format_param = param_list[1] if len(param_list) > 1 else "yyyy-MM-dd"
            value_default = param_list[2] if len(param_list) > 2 else None
            
            # Crear valor por defecto
            if value_default and value_default.upper() not in ['NULL', 'NONE', '']:
                try:
                    default_expr = to_date(lit(value_default), date_format_param)
                except:
                    default_expr = lit(None).cast(DateType())
            else:
                # Si especifica NULL o no hay default, usar null
                default_expr = lit(None).cast(DateType())
            
            # Detectar si es timestamp Unix en millisegundos y convertir a fecha
            return coalesce(
                when(
                    # Es un número (timestamp Unix en millisegundos)
                    col(origin_column).cast(StringType()).rlike("^\\d{10,13}$"),
                    # Convertir de millisegundos Unix a fecha
                    to_date((col(origin_column).cast("bigint") / 1000).cast(TimestampType()))
                ).otherwise(
                    # Intentar convertir con el formato especificado
                    to_date(col(origin_column), date_format_param)
                ),
                # Si falla, usar valor por defecto
                default_expr
            )
        else:
            raise TransformationException(function_name, f"Función no soportada: {function_name}")
    
    def _parse_decimal_type(self, data_type: str) -> DecimalType:
        """Parsea tipo decimal desde string"""
        if isinstance(data_type, str) and "numeric" in data_type.lower():
            match = re.search(r'numeric\((\d+),(\d+)\)', data_type.lower())
            if match:
                precision = int(match.group(1))
                scale = int(match.group(2))
                return DecimalType(precision, scale)
        return DecimalType(38, 12)  # Default

class DeltaTableManager:
    """Maneja operaciones con tablas Delta"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def write_delta_table(self, df, s3_path: str, partition_columns: List[str], 
                         mode: str = "overwrite") -> None:
        """Escribe DataFrame a tabla Delta con optimizaciones válidas"""
        writer = df.write.format("delta").mode(mode)
        
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        
        # Configuraciones Delta válidas
        writer = writer.option("delta.deletedFileRetentionDuration", "interval 7 days")
        writer = writer.option("delta.logRetentionDuration", "interval 30 days")
        
        # Optimización a nivel de Spark (no Delta específico)
        writer = writer.option("spark.sql.adaptive.enabled", "true")
        writer = writer.option("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        writer.save(s3_path)
    
    def merge_delta_table(self, df, s3_path: str, merge_condition: str) -> None:
        """Realiza merge en tabla Delta"""
        delta_table = DeltaTable.forPath(self.spark, s3_path)
        
        # Eliminar duplicados antes del merge para mejorar performance
        df_deduplicated = df.dropDuplicates()
        
        delta_table.alias("old").merge(
            df_deduplicated.alias("new"), 
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    
    def optimize_delta_table(self, s3_path: str) -> None:
        """Optimiza tabla Delta con comandos válidos"""
        try:
            delta_table = DeltaTable.forPath(self.spark, s3_path)
            
            # OPTIMIZE - compacta archivos pequeños
            self.spark.sql(f"OPTIMIZE delta.`{s3_path}`")
            
            # VACUUM - limpia archivos viejos (más de 7 días)
            delta_table.vacuum(168)  # 168 horas = 7 días
            
            # Generar manifest para compatibilidad con otros sistemas
            delta_table.generate("symlink_format_manifest")
            
        except Exception as e:
            logger.warning(f"Error optimizando tabla Delta en {s3_path}: {str(e)}")

class DataProcessor:
    """Procesador principal de datos optimizado con logging integrado"""
    
    def __init__(self, spark_session, config_manager, transformation_engine, delta_manager, logger, dynamo_logger):
        self.spark = spark_session
        self.config_manager = config_manager
        self.transformation_engine = transformation_engine
        self.delta_manager = delta_manager
        self.logger = logger
        self.dynamo_logger = dynamo_logger
        self.now_lima = dt.datetime.now(pytz.utc).astimezone(TZ_LIMA)
    
    def process_table(self, args: Dict[str, str]) -> None:
        """Procesa una tabla completa con logging detallado"""
        table_name = args['TABLE_NAME']
        
        try:
            self.logger.info("🔄 Cargando configuraciones", {"table": table_name})
            
            # Cargar configuraciones
            table_config, endpoint_config, columns_metadata = self._load_configurations(args)
            
            self.logger.info(f"📋 Configuraciones cargadas", {
                "table": table_name,
                "columns_count": len(columns_metadata)
            })
            
            # Construir rutas S3
            s3_paths = self._build_s3_paths(args, table_config)
            self.logger.info(f"📂 Rutas S3 configuradas", {
                "raw_path": s3_paths['raw'],
                "stage_path": s3_paths['stage']
            })
            
            # Leer datos source
            source_df = self._read_source_data(s3_paths['raw'])
            
            if source_df.count() == 0:
                self.logger.warning("⚠️ No se encontraron datos para procesar", {"table": table_name})
                self._handle_empty_data(s3_paths['stage'], columns_metadata)
                
                self.dynamo_logger.log_warning(
                    table_name=table_name,
                    warning_message="No data detected to migrate",
                    job_name=args['JOB_NAME'],
                    context={"empty_data_handled": True}
                )
                return
            
            records_count = source_df.count()
            self.logger.info(f"📊 Datos fuente leídos", {
                "table": table_name,
                "records_count": records_count
            })
            
            # Aplicar transformaciones
            transformed_df, transformation_errors = self.transformation_engine.apply_transformations(
                source_df, columns_metadata
            )
            
            if transformation_errors:
                self.logger.warning(f"⚠️ Errores de transformación detectados", {
                    "table": table_name,
                    "errors_count": len(transformation_errors),
                    "errors": transformation_errors[:3]  # Solo los primeros 3
                })
            
            # Post-procesamiento y escritura
            final_df = self._apply_post_processing(transformed_df, columns_metadata)
            final_count = final_df.count()
            
            self.logger.info(f"🔄 Escribiendo datos transformados", {
                "table": table_name,
                "final_records_count": final_count
            })
            
            self._write_to_stage(final_df, s3_paths['stage'], table_config, columns_metadata)
            
            # Optimizar tabla Delta
            self.delta_manager.optimize_delta_table(s3_paths['stage'])
            self.logger.info("🎯 Tabla Delta optimizada", {"table": table_name})
            
            # Registrar éxito
            self.dynamo_logger.log_success(
                table_name=table_name,
                job_name=args['JOB_NAME'],
                context={
                    "end_time": dt.datetime.now().isoformat(),
                    "records_processed": final_count,
                    "transformation_errors_count": len(transformation_errors),
                    "output_format": "delta"
                }
            )
            
        except Exception as e:
            # Dejar que la excepción se propague para ser manejada en main()
            raise
    
    def _load_configurations(self, args: Dict[str, str]) -> Tuple[TableConfig, EndpointConfig, List[ColumnMetadata]]:
        """Carga todas las configuraciones necesarias"""
        # Cargar datos CSV
        tables_data = self.config_manager.load_csv_from_s3(args['TABLES_CSV_S3'])
        credentials_data = self.config_manager.load_csv_from_s3(args['CREDENTIALS_CSV_S3'])
        columns_data = self.config_manager.load_csv_from_s3(args['COLUMNS_CSV_S3'])
        
        # Encontrar configuración de tabla
        table_config = self._find_table_config(tables_data, args['TABLE_NAME'])
        
        # Encontrar configuración de endpoint
        endpoint_config = self._find_endpoint_config(credentials_data, args['ENDPOINT_NAME'], args['ENVIRONMENT'])
        
        # Procesar metadatos de columnas
        columns_metadata = self._process_columns_metadata(columns_data, args['TABLE_NAME'])
        
        return table_config, endpoint_config, columns_metadata
    
    def _find_table_config(self, tables_data: List[Dict], table_name: str) -> TableConfig:
        """Encuentra configuración de tabla"""
        for row in tables_data:
            if row.get('STAGE_TABLE_NAME', '').upper() == table_name.upper():
                return TableConfig(
                    stage_table_name=row.get('STAGE_TABLE_NAME', ''),
                    source_table=row.get('SOURCE_TABLE', ''),
                    source_table_type=row.get('SOURCE_TABLE_TYPE', 'm'),
                    load_type=row.get('LOAD_TYPE', ''),
                    num_days=row.get('NUM_DAYS'),
                    delay_incremental_ini=row.get('DELAY_INCREMENTAL_INI', '-2')
                )
        raise DataValidationException(f"Configuración de tabla no encontrada: {table_name}")
    
    def _find_endpoint_config(self, credentials_data: List[Dict], endpoint_name: str, environment: str) -> EndpointConfig:
        """Encuentra configuración de endpoint"""
        for row in credentials_data:
            if (row.get('ENDPOINT_NAME', '') == endpoint_name and 
                row.get('ENV', '').upper() == environment.upper()):
                return EndpointConfig(
                    endpoint_name=row.get('ENDPOINT_NAME', ''),
                    environment=row.get('ENV', ''),
                    src_db_name=row.get('SRC_DB_NAME', ''),
                    src_server_name=row.get('SRC_SERVER_NAME', ''),
                    src_db_username=row.get('SRC_DB_USERNAME', '')
                )
        raise DataValidationException(f"Configuración de endpoint no encontrada: {endpoint_name}")
    
    def _process_columns_metadata(self, columns_data: List[Dict], table_name: str) -> List[ColumnMetadata]:
        """Procesa metadatos de columnas"""
        columns_metadata = []
        
        for row in columns_data:
            if row.get('TABLE_NAME', '').upper() == table_name.upper():
                column_meta = ColumnMetadata(
                    name=row.get('COLUMN_NAME', ''),
                    column_id=int(row.get('COLUMN_ID', '0')),
                    data_type=row.get('NEW_DATA_TYPE', 'string'),
                    transformation=row.get('TRANSFORMATION', ''),
                    is_partition=row.get('IS_PARTITION', 'false').lower() in ['true', '1', 'yes', 'y', 't'],
                    is_id=row.get('IS_ID', '').upper() == 'T',
                    is_order_by=row.get('IS_ORDER_BY', '').upper() == 'T',
                    is_filter_date=row.get('IS_FILTER_DATE', '').upper() == 'T'
                )
                columns_metadata.append(column_meta)
        
        return columns_metadata
    
    def _build_s3_paths(self, args: Dict[str, str], table_config: TableConfig) -> Dict[str, str]:
        """Construye rutas S3"""
        now_lima = dt.datetime.now(TZ_LIMA)
        year = now_lima.strftime('%Y')
        month = now_lima.strftime('%m')
        day = now_lima.strftime('%d')
        
        # Extraer nombre limpio de tabla
        source_table_clean = table_config.source_table.split()[0] if ' ' in table_config.source_table else table_config.source_table
        
        day_route = f"{args['TEAM']}/{args['DATA_SOURCE']}/{args['ENDPOINT_NAME']}/{source_table_clean}/year={year}/month={month}/day={day}/"
        
        return {
            'raw': f"s3://{args['S3_RAW_BUCKET']}/{day_route}",
            'stage': f"s3://{args['S3_STAGE_BUCKET']}/{args['TEAM']}/{args['DATA_SOURCE']}/{args['ENDPOINT_NAME']}/{args['TABLE_NAME']}/"
        }
    
    def _read_source_data(self, s3_raw_path: str):
        """Lee datos fuente con cache"""
        try:
            df = self.spark.read.format("parquet").load(s3_raw_path)
            df.cache()  # Cache para optimizar múltiples operaciones
            return df
        except Exception as e:
            logger.error(f"Error leyendo datos desde {s3_raw_path}: {str(e)}")
            # Retornar DataFrame vacío en caso de error
            return self.spark.createDataFrame([], StructType([]))
    
    def _apply_post_processing(self, df, columns_metadata: List[ColumnMetadata]):
        """Aplica post-procesamiento: deduplicación y ordenamiento"""
        # Identificar columnas especiales
        id_columns = [col.name for col in columns_metadata if col.is_id]
        filter_date_columns = [col.name for col in columns_metadata if col.is_filter_date]
        order_by_columns = [col.name for col in columns_metadata if col.is_order_by]
        
        # Deduplicación si hay columnas de fecha de filtro
        if filter_date_columns and id_columns:
            window_spec = Window.partitionBy(*id_columns).orderBy(*[col(c).desc() for c in filter_date_columns])
            df = df.withColumn("row_number", row_number().over(window_spec))
            df = df.filter(col("row_number") == 1).drop("row_number")
        
        # Ordenamiento
        if order_by_columns:
            df = df.orderBy(*order_by_columns)
        
        return df
    
    def _write_to_stage(self, df, s3_stage_path: str, table_config: TableConfig, columns_metadata: List[ColumnMetadata]):
        """Escribe datos a stage"""
        partition_columns = [col.name for col in columns_metadata if col.is_partition]
        
        if DeltaTable.isDeltaTable(self.spark, s3_stage_path):
            if table_config.load_type in ['incremental', 'between-date']:
                # Merge incremental
                id_columns = [col.name for col in columns_metadata if col.is_id]
                merge_condition = " AND ".join([f"old.{col} = new.{col}" for col in id_columns])
                self.delta_manager.merge_delta_table(df, s3_stage_path, merge_condition)
            else:
                # Overwrite completo
                self.delta_manager.write_delta_table(df, s3_stage_path, partition_columns, "overwrite")
        else:
            # Crear nueva tabla
            self.delta_manager.write_delta_table(df, s3_stage_path, partition_columns, "overwrite")
    
    def _handle_empty_data(self, s3_stage_path: str, columns_metadata: List[ColumnMetadata]):
        """Maneja datos vacíos"""
        if not DeltaTable.isDeltaTable(self.spark, s3_stage_path):
            # Crear DataFrame vacío con esquema
            empty_df = self._create_empty_dataframe(columns_metadata)
            partition_columns = [col.name for col in columns_metadata if col.is_partition]
            self.delta_manager.write_delta_table(empty_df, s3_stage_path, partition_columns)
        
        raise Exception("No data detected to migrate")
    
    def _create_empty_dataframe(self, columns_metadata: List[ColumnMetadata]):
        """Crea DataFrame vacío con esquema"""
        fields = []
        for col_meta in sorted(columns_metadata, key=lambda x: x.column_id):
            data_type = self.transformation_engine._get_spark_type(col_meta.data_type)
            fields.append(StructField(col_meta.name, data_type, True))
        
        schema = StructType(fields)
        return self.spark.createDataFrame([], schema)

def setup_logging(table_name: str, team: str, data_source: str):
    """
    Setup DataLakeLogger configuration - siguiendo estándar EXACTO de extract_data_v2
    
    Args:
        table_name: Nombre de la tabla a procesar
        team: Equipo propietario
        data_source: Fuente de datos
    """
    DataLakeLogger.configure_global(
        log_level=logging.INFO,
        service_name="light_transform",
        correlation_id=f"{team}-{data_source}-light_transform-{table_name}",
        owner=team,
        auto_detect_env=True,
        force_local_mode=False
    )

def main():
    """Función principal optimizada con logging integrado"""
    logger = None
    monitor = None
    dynamo_logger = None
    process_id = None
    
    try:
        # Obtener argumentos de Glue
        args = getResolvedOptions(
            sys.argv, 
            ['JOB_NAME', 'S3_RAW_BUCKET', 'S3_STAGE_BUCKET', 'DYNAMO_LOGS_TABLE', 
             'TABLE_NAME', 'ARN_TOPIC_FAILED', 'PROJECT_NAME', 'TEAM', 'DATA_SOURCE', 
             'TABLES_CSV_S3', 'CREDENTIALS_CSV_S3', 'COLUMNS_CSV_S3', 'ENDPOINT_NAME', 'ENVIRONMENT']
        )
        
        # Configurar DataLakeLogger globalmente
        setup_logging(
            table_name=args['TABLE_NAME'],
            team=args.get('TEAM'),
            data_source=args.get('DATA_SOURCE')
        )
        
        # Obtener logger principal
        logger = DataLakeLogger.get_logger(__name__)
        
        # Configurar DynamoDB Logger
        dynamo_logger = DynamoDBLogger(
            table_name=args.get("DYNAMO_LOGS_TABLE"),
            sns_topic_arn=args.get("ARN_TOPIC_FAILED"),
            team=args.get("TEAM"),
            data_source=args.get("DATA_SOURCE"),
            endpoint_name=args.get("ENDPOINT_NAME"),
            flow_name="light_transform",
            environment=args.get("ENVIRONMENT"),
            logger_name=f"{args.get('TEAM')}-transform-dynamo"
        )
        
        monitor = Monitor(dynamo_logger)

        process_id = monitor.log_start(
            table_name=args['TABLE_NAME'],
            job_name=args['JOB_NAME'],
            context={
                "start_time": dt.datetime.now().isoformat(),
                "environment": args.get('ENVIRONMENT'),
                "team": args.get('TEAM'),
                "data_source": args.get('DATA_SOURCE'),
                "endpoint_name": args.get('ENDPOINT_NAME')  # 👈 endpoint en contexto
            }
        )

        logger.info("🚀 Iniciando Light Transform", {
            "table": args['TABLE_NAME'],
            "job": args['JOB_NAME'],
            "team": args['TEAM'],
            "data_source": args['DATA_SOURCE'],
            "endpoint": args.get('ENDPOINT_NAME'),
            "process_id": process_id
        })
        
        # Configurar Spark con optimizaciones válidas
        spark = SparkSession.builder \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        # Configurar sistema de archivos S3
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        
        # Registrar inicio del proceso
        process_id = dynamo_logger.log_start(
            table_name=args['TABLE_NAME'],
            job_name=args['JOB_NAME'],
            context={
                "start_time": dt.datetime.now().isoformat(),
                "s3_raw_bucket": args['S3_RAW_BUCKET'],
                "s3_stage_bucket": args['S3_STAGE_BUCKET'],
                "endpoint_name": args['ENDPOINT_NAME']
            }
        )
        
        # Inicializar componentes
        s3_client = boto3.client('s3')
        config_manager = ConfigurationManager(s3_client)
        transformation_engine = TransformationEngine(spark)
        delta_manager = DeltaTableManager(spark)
        
        # Procesar tabla con logging integrado
        processor = DataProcessor(
            spark,
            config_manager,
            transformation_engine,
            delta_manager,
            logger,
            dynamo_logger
        )
        
        processor.process_table(args)
        
        logger.info("✅ Light Transform completado exitosamente", {
            "table": args['TABLE_NAME'],
            "process_id": process_id
        })
        
    except Exception as e:
        error_msg = str(e)
        if logger:
            logger.error(f"❌ Error en Light Transform: {error_msg}", {
                "table": args.get('TABLE_NAME', 'unknown'),
                "job": args.get('JOB_NAME', 'unknown'),
                "error_type": type(e).__name__
            })
        
        # Registrar error en DynamoDB (esto enviará SNS automáticamente)
        if dynamo_logger:
            dynamo_logger.log_failure(
                table_name=args.get('TABLE_NAME', 'unknown'),
                error_message=error_msg,
                job_name=args.get('JOB_NAME', 'unknown'),
                context={
                    "error_type": type(e).__name__,
                    "failed_at": dt.datetime.now().isoformat()
                }
            )
        
        # El job debe terminar exitosamente para evitar dobles notificaciones
        if logger:
            logger.info("ℹ️ Job terminando como SUCCESS para evitar dobles notificaciones")
        
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()