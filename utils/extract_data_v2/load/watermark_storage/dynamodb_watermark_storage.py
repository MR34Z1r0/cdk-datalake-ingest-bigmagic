# load/watermark_storage/dynamodb_watermark_storage.py
from interfaces.watermark_interface import WatermarkStorageInterface
from aje_libs.common.helpers.dynamodb_helper import DynamoDBHelper
from utils.date_utils import get_current_lima_time
from aje_libs.common.logger import custom_logger
from typing import Optional, Dict, Any, List
import json
from datetime import timedelta

class DynamoDBWatermarkStorage(WatermarkStorageInterface):
    """Implementación DynamoDB para watermarks"""
    
    def __init__(self, table_name: str, project_name: str):
        self.logger = custom_logger(__name__)
        self.table_name = table_name
        self.project_name = project_name
        self.dynamo_helper = DynamoDBHelper(
            table_name=table_name,
            pk_name="WATERMARK_KEY",
            sk_name="TIMESTAMP"
        )
    
    def get_last_extracted_value(self, table_name: str, column_name: str) -> Optional[str]:
        """Obtiene el último watermark de DynamoDB"""
        try:
            watermark_key = f"{self.project_name}#{table_name}#{column_name}"
            
            # ✅ CORRECCIÓN: Usar expression attribute values
            response = self.dynamo_helper.query_table(
                key_condition="WATERMARK_KEY = :pk",
                expression_attribute_values={':pk': watermark_key},
                limit=1,
                scan_forward=False  # Más reciente primero
            )
            
            if response:
                return response[0].get('EXTRACTED_VALUE')
            
            return None
            
        except Exception as e: 
            self.logger.warning(f"Failed to read watermark from DynamoDB: {e}")
            return None
    
    def _preserve_microseconds(self, value: str) -> str:
        """Preserva precisión según el tipo de dato"""
        try:
            value_str = str(value).strip()
            
            # Si es una fecha/timestamp, preservar microsegundos
            if any(char in value_str for char in ['-', ':', ' ', '.']):
                from datetime import datetime
                
                for fmt in ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
                    try:
                        dt = datetime.strptime(value_str, fmt)
                        return dt.strftime('%Y-%m-%d %H:%M:%S.%f')
                    except ValueError:
                        continue
            
            # Si es número, preservar tal como está
            try:
                # Verificar que es un número válido
                float(value_str)
                return value_str
            except ValueError:
                pass
            
            # Fallback: retornar como string
            return value_str
            
        except Exception:
            return str(value)

    def set_last_extracted_value(self, table_name: str, column_name: str, 
                               value: str, metadata: Dict[str, Any] = None) -> bool:
        """Guarda watermark en DynamoDB"""
        try:
            now = get_current_lima_time()
            watermark_key = f"{self.project_name}#{table_name}#{column_name}"
            
            # Asegurar que el valor preserve microsegundos
            preserved_value = self._preserve_microseconds(value)

            watermark_entry = {
                'WATERMARK_KEY': watermark_key,
                'TIMESTAMP': now.strftime('%Y-%m-%d %H:%M:%S.%f'),  # Con microsegundos
                'TABLE_NAME': table_name,
                'COLUMN_NAME': column_name,
                'EXTRACTED_VALUE': preserved_value,  # Valor con microsegundos preservados
                'PROJECT_NAME': self.project_name,
                'METADATA': json.dumps(metadata or {}),
                'TTL': int((now + timedelta(days=90)).timestamp())
            }
            
            self.dynamo_helper.put_item(watermark_entry)
            return True
            
        except Exception as e: 
            self.logger.error(f"Failed to save watermark to DynamoDB: {e}")
            return False

    def get_extraction_history(self, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Obtiene el historial de extracciones"""
        try:
            # ✅ CORRECCIÓN: Usar begins_with con expression attribute values
            watermark_prefix = f"{self.project_name}#{table_name}#"
            
            # Intentar query primero si el patrón lo permite
            try:
                response = self.dynamo_helper.query_table(
                    key_condition="begins_with(WATERMARK_KEY, :prefix)",
                    expression_attribute_values={':prefix': watermark_prefix},
                    limit=limit,
                    scan_forward=False
                )
            except:
                # Si query falla, usar scan
                response = self.dynamo_helper.scan_table(
                    filter_expression="begins_with(WATERMARK_KEY, :prefix)",
                    expression_attribute_values={':prefix': watermark_prefix},
                    limit=limit
                )
            
            # Formatear respuesta
            history = []
            for item in response:
                history.append({
                    'table_name': item.get('TABLE_NAME'),
                    'column_name': item.get('COLUMN_NAME'),
                    'extracted_value': item.get('EXTRACTED_VALUE'),
                    'timestamp': item.get('TIMESTAMP'),
                    'metadata': json.loads(item.get('METADATA', '{}'))
                })
            
            # Ordenar por timestamp
            history.sort(key=lambda x: x['timestamp'], reverse=True)
            return history[:limit]
            
        except Exception as e:
            self.logger.warning(f"Failed to read extraction history from DynamoDB: {e}")
            return []

    def cleanup_old_watermarks(self, days_to_keep: int = 90) -> int:
        """Limpia watermarks antiguos usando TTL"""
        # En DynamoDB, esto se maneja automáticamente con TTL
        # Solo retornamos 0 porque el cleanup es automático
        self.logger.info(f"DynamoDB TTL handles automatic cleanup of records older than {days_to_keep} days")
        return 0
    
    def get_watermarks_by_project(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Obtiene todos los watermarks de un proyecto"""
        try:
            # ✅ CORRECCIÓN: Usar expression attribute values
            project_prefix = f"{self.project_name}#"
            
            response = self.dynamo_helper.scan_table(
                filter_expression="begins_with(WATERMARK_KEY, :prefix)",
                expression_attribute_values={':prefix': project_prefix},
                limit=limit
            )
            
            # Formatear respuesta
            watermarks = []
            for item in response:
                watermarks.append({
                    'watermark_key': item.get('WATERMARK_KEY'),
                    'table_name': item.get('TABLE_NAME'),
                    'column_name': item.get('COLUMN_NAME'),
                    'extracted_value': item.get('EXTRACTED_VALUE'),
                    'timestamp': item.get('TIMESTAMP'),
                    'project_name': item.get('PROJECT_NAME'),
                    'metadata': json.loads(item.get('METADATA', '{}'))
                })
            
            # Ordenar por timestamp
            watermarks.sort(key=lambda x: x['timestamp'], reverse=True)
            return watermarks
            
        except Exception as e:
            self.logger.error(f"Failed to get watermarks by project: {e}")
            return []
    
    def delete_watermark(self, table_name: str, column_name: str, timestamp: str) -> bool:
        """Elimina un watermark específico"""
        try:
            watermark_key = f"{self.project_name}#{table_name}#{column_name}"
            
            # Eliminar item específico
            self.dynamo_helper.delete_item(
                partition_key=watermark_key,
                sort_key=timestamp
            )
            
            self.logger.info(f"Deleted watermark: {watermark_key} at {timestamp}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to delete watermark: {e}")
            return False
    
    def update_watermark_metadata(self, table_name: str, column_name: str, 
                                 new_metadata: Dict[str, Any]) -> bool:
        """Actualiza los metadatos del último watermark"""
        try:
            # Primero obtener el último watermark
            last_value = self.get_last_extracted_value(table_name, column_name)
            if not last_value:
                self.logger.warning(f"No watermark found to update for {table_name}.{column_name}")
                return False
            
            # Obtener el timestamp del último registro
            watermark_key = f"{self.project_name}#{table_name}#{column_name}"
            
            response = self.dynamo_helper.query_table(
                key_condition="WATERMARK_KEY = :pk",
                expression_attribute_values={':pk': watermark_key},
                limit=1,
                scan_forward=False
            )
            
            if not response:
                return False
            
            last_timestamp = response[0].get('TIMESTAMP')
            
            # ✅ CORRECCIÓN: Actualizar con expression attribute values
            self.dynamo_helper.update_item(
                partition_key=watermark_key,
                sort_key=last_timestamp,
                update_expression="SET METADATA = :metadata",
                expression_attribute_values={':metadata': json.dumps(new_metadata)}
            )
            
            self.logger.info(f"Updated watermark metadata for {table_name}.{column_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update watermark metadata: {e}")
            return False