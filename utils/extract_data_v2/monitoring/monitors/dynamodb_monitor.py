# monitoring/monitors/dynamodb_monitor.py
from typing import Dict, Any
from interfaces.monitor_interface import MonitorInterface
from models.extraction_result import ExtractionResult
from aje_libs.common.dynamodb_logger import DynamoDBLogger
from aje_libs.common.datalake_logger import DataLakeLogger

class DynamoDBMonitor(MonitorInterface):
    """
    DynamoDB implementation of MonitorInterface
    Usa DynamoDBLogger internamente para centralizar toda la lógica de logging
    """
    
    def __init__(self, table_name: str, project_name: str, **kwargs):
        self.table_name = table_name
        self.project_name = project_name
        
        # Extraer configuración
        team = kwargs.get('team', 'default')
        data_source = kwargs.get('data_source', 'unknown')
        environment = kwargs.get('environment', 'DEV')
        sns_topic_arn = kwargs.get('sns_topic_arn')
        
        # ÚNICA INSTANCIA de DynamoDBLogger - aquí se centraliza TODO
        self.dynamo_logger = DynamoDBLogger(
            table_name=table_name,
            sns_topic_arn=sns_topic_arn,
            team=team,
            data_source=data_source,
            flow_name='extract_data',
            environment=environment,
            logger_name=f"{project_name}-monitor"
        )
        
        # Logger local para debug del monitor
        self.logger = DataLakeLogger.get_logger(
            name=f"{project_name}-dynamodb-monitor",
            service_name="extract_data_monitoring"
        )
        
        self.logger.info(
            f"DynamoDBMonitor inicializado usando DynamoDBLogger",
            {"table": table_name, "project": project_name}
        )
    
    def log_start(self, table_name: str, strategy: str, metadata: Dict[str, Any] = None) -> str:
        """Log extraction start"""
        try:
            context = self._build_context(table_name, strategy, metadata or {})
            
            process_id = self.dynamo_logger.log_start(
                table_name=table_name,
                job_name=f"extract_{strategy}",
                context=context
            )
            
            self.logger.info(
                f"Inicio de extracción registrado",
                {"table": table_name, "strategy": strategy, "process_id": process_id}
            )
            
            return process_id
            
        except Exception as e:
            self.logger.error(f"Error al registrar inicio: {e}")
            return ""
    
    def log_success(self, result: ExtractionResult) -> str:
        """Log successful extraction"""
        try:
            context = {
                'records_extracted': result.records_extracted,
                'files_created_count': len(result.files_created),
                'files_created': result.files_created[:10] if result.files_created else [],  # Primeros 10
                'execution_time_seconds': result.execution_time_seconds,
                'strategy_used': result.strategy_used,
                'start_time': result.start_time.isoformat() if result.start_time else None,
                'end_time': result.end_time.isoformat() if result.end_time else None,
                'project_name': self.project_name,
                'process_type': self._get_process_type(result.strategy_used),
                **(result.metadata or {})
            }
            
            process_id = self.dynamo_logger.log_success(
                table_name=result.table_name,
                job_name=f"extract_{result.strategy_used}",
                context=context
            )
            
            self.logger.info(
                f"Extracción exitosa registrada",
                {
                    "table": result.table_name,
                    "records": result.records_extracted,
                    "files": len(result.files_created),
                    "process_id": process_id
                }
            )
            
            return process_id
            
        except Exception as e:
            self.logger.error(f"Error al registrar éxito: {e}")
            return ""
    
    def log_error(self, table_name: str, error_message: str, metadata: Dict[str, Any] = None) -> str:
        """Log extraction error"""
        try:
            context = self._build_context(table_name, "error", metadata or {})
            context['error_details'] = error_message
            context['project_name'] = self.project_name
            
            process_id = self.dynamo_logger.log_failure(
                table_name=table_name,
                error_message=error_message,
                job_name="extract_data",
                context=context
            )
            
            self.logger.error(
                f"Error registrado",
                {"table": table_name, "error": error_message[:200], "process_id": process_id}
            )
            
            return process_id
            
        except Exception as e:
            self.logger.error(f"Error al registrar fallo: {e}")
            return ""
    
    def log_warning(self, table_name: str, warning_message: str, metadata: Dict[str, Any] = None) -> str:
        """Log extraction warning"""
        try:
            context = self._build_context(table_name, "warning", metadata or {})
            context['warning_details'] = warning_message
            context['project_name'] = self.project_name
            
            process_id = self.dynamo_logger.log_warning(
                table_name=table_name,
                message=warning_message,
                job_name="extract_data",
                context=context
            )
            
            self.logger.warning(
                f"Warning registrado",
                {"table": table_name, "warning": warning_message[:200], "process_id": process_id}
            )
            
            return process_id
            
        except Exception as e:
            self.logger.error(f"Error al registrar warning: {e}")
            return ""
    
    def send_notification(self, message: str, is_error: bool = False):
        """
        Send notification via SNS
        Nota: DynamoDBLogger ya envía notificaciones automáticamente en log_failure
        Este método se mantiene por compatibilidad con la interfaz
        """
        if is_error:
            self.logger.info("Notificación de error ya enviada automáticamente por DynamoDBLogger")
        else:
            # Para notificaciones informativas, podrías implementar lógica adicional aquí
            self.logger.info(f"Notificación informativa: {message[:100]}")
    
    def _get_process_type(self, strategy: str) -> str:
        """Determina el tipo de proceso basado en la estrategia"""
        strategy_lower = strategy.lower()
        if 'full' in strategy_lower or 'completa' in strategy_lower:
            return 'F'
        elif 'incremental' in strategy_lower:
            return 'I'
        elif 'partition' in strategy_lower or 'particion' in strategy_lower:
            return 'P'
        else:
            return 'U'  # Unknown
    
    def _build_context(self, table_name: str, strategy: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Construye el contexto para el log"""
        context = {
            'table': table_name,
            'server': metadata.get('server', ''),
            'user': metadata.get('user', ''),
            'database': metadata.get('database', ''),
            'strategy': strategy,
            'process_type': self._get_process_type(strategy),
            'project_name': self.project_name,
            **metadata
        }
        
        # Limpiar valores None y vacíos
        return {k: v for k, v in context.items() if v not in (None, '', [])}