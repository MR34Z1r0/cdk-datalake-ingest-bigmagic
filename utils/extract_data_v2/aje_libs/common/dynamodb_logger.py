# utils/extract_data_v2/aje_libs/common/dynamodb_logger.py

import json
import boto3
import pytz
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, Optional
from botocore.exceptions import ClientError
from .datalake_logger import DataLakeLogger
  
def sanitize_for_dynamodb(obj):
    """
    Sanitiza objetos para DynamoDB convirtiendo tipos no soportados.
    
    - float -> Decimal
    - inf/nan -> None
    - Recursivo para dict y list
    """
    if isinstance(obj, dict):
        return {k: sanitize_for_dynamodb(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_dynamodb(item) for item in obj]
    elif isinstance(obj, float):
        # Manejar casos especiales
        if obj != obj or obj == float('inf') or obj == float('-inf'):
            return None
        return Decimal(str(obj))
    elif isinstance(obj, set):
        return {sanitize_for_dynamodb(item) for item in obj if item is not None}
    return obj

class DynamoDBLogger:
    """
    Logger para DynamoDB que se integra con DataLakeLogger
    Registra logs de proceso y envía notificaciones SNS en caso de errores
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
        logger_name: Optional[str] = None,
        process_guid: Optional[str] = None
    ):
        """
        Inicializa el DynamoDB Logger
        
        Args:
            table_name: Nombre de la tabla DynamoDB para logs
            sns_topic_arn: ARN del topic SNS para notificaciones de error
            team: Nombre del equipo
            data_source: Fuente de datos
            flow_name: Nombre del flujo (extract_data_v2, light_transform)
            environment: Ambiente (DEV, PROD, etc.)
            region: Región AWS
            logger_name: Nombre del logger para DataLakeLogger
        """
        self.table_name = table_name
        self.sns_topic_arn = sns_topic_arn
        self.team = team
        self.data_source = data_source
        self.endpoint_name = endpoint_name
        self.flow_name = flow_name
        self.environment = environment
        self.process_guid = process_guid or str(uuid.uuid4())
        
        # Configurar timezone Lima
        self.tz_lima = pytz.timezone('America/Lima')
        
        # Obtener logger usando DataLakeLogger
        self.logger = DataLakeLogger.get_logger(
            name=logger_name or f"{team}-{flow_name}",
            service_name=f"{team}-{flow_name}"
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
        """
        Registra el estatus de un proceso en DynamoDB
        
        Args:
            status: Estado del proceso (RUNNING, SUCCESS, FAILED, WARNING)
            message: Mensaje descriptivo
            table_name: Nombre de la tabla que se está procesando
            job_name: Nombre del job
            context: Contexto adicional
            
        Returns:
            process_id: ID único del proceso registrado
        """
        if not self.dynamodb_table:
            self.logger.warning(f"DynamoDB no configurado, log no registrado: {status} - {message}")
            return ""
        
        try:
            # Generar timestamp y process_id únicos
            now_lima = datetime.now(pytz.utc).astimezone(self.tz_lima)
            timestamp = now_lima.strftime("%Y%m%d_%H%M%S_%f")
            process_id = f"{self.team}-{self.data_source}-{self.endpoint_name}-{table_name}"
            
            # Preparar contexto con límites de tamaño
            log_context = self._prepare_enhanced_context(context or {}, status, table_name)
             
            truncated_message = message
            
            # Crear registro compatible con tu estructura existente
            record = {
                "PROCESS_ID": process_id,
                "PROCESS_GUID": self.process_guid,
                "DATE_SYSTEM": timestamp,
                "RESOURCE_NAME": job_name or "unknown_job",
                "RESOURCE_TYPE": "python_shell_glue_job" if "glue" in self.flow_name.lower() else "python_process",
                "STATUS": status.upper(),
                "MESSAGE": truncated_message,
                "PROCESS_TYPE": self._get_process_type(status),
                "CONTEXT": log_context,
                "TEAM": self.team,
                "DATASOURCE": self.data_source,
                "ENDPOINT_NAME": self.endpoint_name,
                "TABLE_NAME": table_name,
                "ENVIRONMENT": self.environment,
                "LOG_CREATED_AT": now_lima.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Insertar en DynamoDB
            self.dynamodb_table.put_item(Item=record)
            self.logger.info(f"Log registrado en DynamoDB", {
                "process_id": process_id, 
                "process_guid": self.process_guid,
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
        
        #def truncate_data(data, max_length=1000):
        #    """Trunca estructuras de datos"""
        #    if isinstance(data, str):
        #        return data[:max_length] + "...[TRUNCATED]" if len(data) > max_length else data
        #    elif isinstance(data, dict):
        #        truncated = {}
        #        for k, v in list(data.items())[:10]:
        #            truncated[k] = truncate_data(v, 500)
        #        if len(data) > 10:
        #            truncated["_truncated_items"] = f"...and {len(data) - 10} more items"
        #        return truncated
        #    elif isinstance(data, list):
        #        truncated = [truncate_data(item, 200) for item in data[:5]]
        #        if len(data) > 5:
        #            truncated.append(f"...and {len(data) - 5} more items")
        #        return truncated
        #    else:
        #        return str(data)[:500] if data else data
        
        #prepared_context = truncate_data(context)
        sanitized_context = sanitize_for_dynamodb(context)
    
        prepared_context = sanitized_context
        
        # Verificar tamaño total
        context_json = json.dumps(prepared_context, default=str)
        if len(context_json.encode("utf-8")) > MAX_CONTEXT_SIZE:
            return {
                "size_limit_applied": "Context truncated due to DynamoDB size limits",
                "original_keys": list(context.keys())[:10],
                "truncated_at": datetime.now(self.tz_lima).strftime("%Y-%m-%d %H:%M:%S")
            }
        
        return prepared_context
    
    def _prepare_enhanced_context(self, context: Dict[str, Any], status: str, table_name: str) -> Dict[str, Any]:
        """
        Prepara el contexto con datos adicionales específicos por estado para analítica
        
        Args:
            context: Contexto original
            status: Estado del proceso
            table_name: Nombre de la tabla
            
        Returns:
            Contexto enriquecido con datos por estado
        """
        # Preparar contexto base con límites de tamaño
        enhanced_context = self._prepare_context(context)
        
        # Agregar metadata común para todos los estados
        enhanced_context.update({
            "status_type": status.upper(),
            "execution_timestamp": datetime.now(self.tz_lima).isoformat(),
            "pipeline_component": self.flow_name,
            "source_system": self.data_source
        })
        
        # Datos específicos por estado para analítica
        if status.upper() == "RUNNING":
            enhanced_context.update({
                "start_time": context.get("start_time", datetime.now(self.tz_lima).isoformat()),
                "expected_duration_minutes": context.get("expected_duration_minutes"),
                "batch_size": context.get("batch_size"),
                "parallel_workers": context.get("parallel_workers")
            })
        
        elif status.upper() == "SUCCESS":
            success_data = {
                "end_time": context.get("end_time", datetime.now(self.tz_lima).isoformat()),
                "success_rate": context.get("success_rate", "100%")
            }
            if context.get("duration_seconds"):
                success_data["duration_seconds"] = context.get("duration_seconds")
            if context.get("records_processed"):
                success_data["records_processed"] = context.get("records_processed")
            if context.get("data_size_mb"):
                success_data["data_size_mb"] = context.get("data_size_mb")
            
            enhanced_context.update(success_data)
        
        elif status.upper() == "FAILED":
            enhanced_context.update({
                "error_time": context.get("error_time", datetime.now(self.tz_lima).isoformat()),
                "error_type": context.get("error_type", "UnknownError"),
                "error_details": context.get("error_details", ""),
                "failed_at_step": context.get("failed_at_step"),
                "records_processed_before_failure": context.get("records_processed", 0),
                "retry_count": context.get("retry_count", 0),
                "is_retryable": context.get("is_retryable", False),
                "stack_trace": context.get("stack_trace", "")[:1000]  # Limitado para no exceder tamaño
            })
        
        elif status.upper() == "WARNING":
            enhanced_context.update({
                "warning_time": context.get("warning_time", datetime.now(self.tz_lima).isoformat()),
                "warning_type": context.get("warning_type", "GeneralWarning"),
                "warning_details": context.get("warning_details", ""),
                "affected_records": context.get("affected_records", 0),
                "records_with_issues": context.get("records_with_issues", 0),
                "data_quality_score": context.get("data_quality_score"),
                "can_continue": context.get("can_continue", True),
                "remediation_applied": context.get("remediation_applied", False)
            })
        
        return enhanced_context

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
🚨 PROCESO FALLIDO EN DATA PIPELINE

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
3. Verifica la configuración de la tabla y conexiones

⚠️ Este mensaje se envía automáticamente. El job se marca como SUCCESS para evitar dobles notificaciones.
            """
            
            # Enviar notificación
            self.sns_client.publish(
                TopicArn=self.sns_topic_arn,
                Subject=f"🚨 [ERROR] {self.flow_name.upper()} - {record.get('TABLE_NAME')} - {record.get('TEAM')}",
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