# -*- coding: utf-8 -*-
from typing import Dict, Any
from interfaces.monitor_interface import MonitorInterface
from models.extraction_result import ExtractionResult
from aje_libs.common.helpers.dynamodb_helper import DynamoDBHelper
from utils.date_utils import get_current_lima_time
import boto3

class DynamoDBMonitor(MonitorInterface):
    """DynamoDB implementation of MonitorInterface"""
    
    def __init__(self, table_name: str, project_name: str, **kwargs):
        self.table_name = table_name
        self.project_name = project_name
        self.sns_topic_arn = kwargs.get('sns_topic_arn')
        
        # Initialize DynamoDB helper
        self.dynamo_helper = DynamoDBHelper(table_name, "PROCESS_ID", None)
        
        # Initialize SNS client if topic provided
        self.sns_client = boto3.client("sns") if self.sns_topic_arn else None
    
    def log_start(self, table_name: str, strategy: str, metadata: Dict[str, Any] = None):
        """Log extraction start"""
        try:
            now = get_current_lima_time()
            process_id = self._generate_process_id(table_name, now)
            
            log_entry = {
                'PROCESS_ID': process_id,
                'DATE_SYSTEM': now.strftime('%Y%m%d_%H%M%S'),
                'PROJECT_NAME': self.project_name,
                'FLOW_NAME': 'extract_data',
                'TASK_NAME': 'extract_data_table',
                'TASK_STATUS': 'iniciado',
                'MESSAGE': f'Iniciando extracción con estrategia: {strategy}',
                'PROCESS_TYPE': self._get_process_type(strategy),
                'CONTEXT': self._build_context(table_name, metadata or {})
            }
            
            self.dynamo_helper.put_item(log_entry)
            
        except Exception as e:
            # Don't fail the main process if logging fails
            print(f"Warning: Failed to log start: {e}")
    
    def log_success(self, result: ExtractionResult):
        """Log successful extraction"""
        try:
            now = get_current_lima_time()
            process_id = self._generate_process_id(result.table_name, now)
            
            log_entry = {
                'PROCESS_ID': process_id,
                'DATE_SYSTEM': now.strftime('%Y%m%d_%H%M%S'),
                'PROJECT_NAME': self.project_name,
                'FLOW_NAME': 'extract_data',
                'TASK_NAME': 'extract_data_table',
                'TASK_STATUS': 'satisfactorio',
                'MESSAGE': f'Extracción completada. Registros: {result.records_extracted}',
                'PROCESS_TYPE': self._get_process_type(result.strategy_used),
                'CONTEXT': self._build_context(result.table_name, result.metadata or {})
            }
            
            self.dynamo_helper.put_item(log_entry)
            
        except Exception as e:
            print(f"Warning: Failed to log success: {e}")
    
    def log_error(self, table_name: str, error_message: str, metadata: Dict[str, Any] = None):
        """Log extraction error"""
        try:
            now = get_current_lima_time()
            process_id = self._generate_process_id(table_name, now)
            
            log_entry = {
                'PROCESS_ID': process_id,
                'DATE_SYSTEM': now.strftime('%Y%m%d_%H%M%S'),
                'PROJECT_NAME': self.project_name,
                'FLOW_NAME': 'extract_data',
                'TASK_NAME': 'extract_data_table',
                'TASK_STATUS': 'error',
                'MESSAGE': error_message,
                'PROCESS_TYPE': 'F',  # Default to full for errors
                'CONTEXT': self._build_context(table_name, metadata or {})
            }
            
            self.dynamo_helper.put_item(log_entry)
            
        except Exception as e:
            print(f"Warning: Failed to log error: {e}")
    
    def send_notification(self, message: str, is_error: bool = False):
        """Send notification via SNS"""
        if not self.sns_client or not self.sns_topic_arn:
            return
        
        try:
            self.sns_client.publish(
                TopicArn=self.sns_topic_arn,
                Message=message
            )
        except Exception as e:
            print(f"Warning: Failed to send notification: {e}")
    
    def _generate_process_id(self, table_name: str, timestamp) -> str:
        """Generate process ID"""
        # Extract clean table name
        clean_table_name = table_name.split()[0] if ' ' in table_name else table_name
        table_prefix = table_name.split('_')[0] if '_' in table_name else table_name[:3]
        
        return f"DLB_{table_prefix.upper()}_{clean_table_name}_{timestamp.strftime('%Y%m%d_%H%M%S')}"
    
    def _get_process_type(self, strategy: str) -> str:
        """Get process type code based on strategy"""
        incremental_strategies = ['incremental', 'date_range']
        return 'D' if strategy.lower() in incremental_strategies else 'F'
    
    def _build_context(self, table_name: str, metadata: Dict[str, Any]) -> str:
        """Build context string for logging"""
        context_parts = [f"table='{table_name}'"]
        
        if 'server' in metadata:
            context_parts.append(f"server='{metadata['server']}'")
        
        if 'username' in metadata:
            context_parts.append(f"user='{metadata['username']}'")
        
        if 'strategy' in metadata:
            context_parts.append(f"strategy='{metadata['strategy']}'")
        
        return "{" + ", ".join(context_parts) + "}"