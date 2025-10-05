# load/watermark_storage/transactional_watermark_storage.py
from interfaces.watermark_interface import WatermarkStorageInterface
from aje_libs.common.datalake_logger import DataLakeLogger
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum

class WatermarkStatus(Enum):
    """Estados de watermark"""
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    ROLLBACK = "ROLLBACK"

class TransactionalWatermarkStorage(WatermarkStorageInterface):
    """Wrapper transaccional sobre WatermarkStorage"""
    
    def __init__(self, base_storage: WatermarkStorageInterface, project_name: str):
        self.logger = DataLakeLogger.get_logger(__name__)
        self.base_storage = base_storage
        self.project_name = project_name
        self._pending_watermarks = {}
        self.logger.info("✅ TransactionalWatermarkStorage initialized")
    
    def get_last_extracted_value(self, table_name: str, column_name: str) -> Optional[str]:
        """Obtiene último watermark CONFIRMADO (ignora PENDING)"""
        try:
            history = self.base_storage.get_extraction_history(table_name, limit=50)
            
            for entry in history:
                if entry.get('column_name') == column_name:
                    metadata = entry.get('metadata', {})
                    status = metadata.get('status', 'CONFIRMED')
                    
                    if status == WatermarkStatus.CONFIRMED.value:
                        value = entry.get('extracted_value')
                        self.logger.info(f"✅ Found CONFIRMED watermark: {table_name}.{column_name} = {value}")
                        return value
            
            self.logger.info(f"ℹ️ No CONFIRMED watermark for {table_name}.{column_name}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting watermark: {e}")
            return None
    
    def save_provisional(self, table_name: str, column_name: str, value: str, metadata: Dict[str, Any] = None) -> bool:
        """Guarda watermark PROVISIONAL (PENDING)"""
        try:
            full_metadata = {
                **(metadata or {}),
                'status': WatermarkStatus.PENDING.value,
                'saved_at': datetime.now().isoformat(),
                'transaction_id': f"{table_name}_{column_name}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
            }
            
            success = self.base_storage.set_last_extracted_value(
                table_name=table_name,
                column_name=column_name,
                value=value,
                metadata=full_metadata
            )
            
            if success:
                cache_key = f"{table_name}#{column_name}"
                self._pending_watermarks[cache_key] = {
                    'value': value,
                    'metadata': full_metadata,
                    'saved_at': datetime.now()
                }
                self.logger.info(f"💾 PENDING watermark saved: {table_name}.{column_name} = {value}")
                return True
            
            return False
                
        except Exception as e:
            self.logger.error(f"Error saving provisional: {e}")
            return False
    
    def confirm(self, table_name: str, column_name: str, additional_metadata: Dict[str, Any] = None) -> bool:
        """Confirma watermark PENDING → CONFIRMED"""
        try:
            cache_key = f"{table_name}#{column_name}"
            
            if cache_key not in self._pending_watermarks:
                self.logger.warning(f"⚠️ No PENDING watermark to confirm: {table_name}.{column_name}")
                return False
            
            pending = self._pending_watermarks[cache_key]
            
            confirmed_metadata = {
                **pending['metadata'],
                'status': WatermarkStatus.CONFIRMED.value,
                'confirmed_at': datetime.now().isoformat(),
                **(additional_metadata or {})
            }
            
            success = self.base_storage.set_last_extracted_value(
                table_name=table_name,
                column_name=column_name,
                value=pending['value'],
                metadata=confirmed_metadata
            )
            
            if success:
                del self._pending_watermarks[cache_key]
                self.logger.info(f"✅ CONFIRMED watermark: {table_name}.{column_name} = {pending['value']}")
                return True
            
            return False
                
        except Exception as e:
            self.logger.error(f"Error confirming: {e}")
            return False
    
    def rollback(self, table_name: str, column_name: str, error_info: Dict[str, Any] = None) -> bool:
        """Revierte watermark PENDING → ROLLBACK"""
        try:
            cache_key = f"{table_name}#{column_name}"
            
            if cache_key in self._pending_watermarks:
                pending = self._pending_watermarks[cache_key]
                
                rollback_metadata = {
                    **pending['metadata'],
                    'status': WatermarkStatus.ROLLBACK.value,
                    'rollback_at': datetime.now().isoformat(),
                    'error_info': error_info or {}
                }
                
                self.base_storage.set_last_extracted_value(
                    table_name=table_name,
                    column_name=column_name,
                    value=pending['value'],
                    metadata=rollback_metadata
                )
                
                del self._pending_watermarks[cache_key]
                self.logger.warning(f"🔄 ROLLBACK watermark: {table_name}.{column_name}")
                return True
            
            self.logger.info(f"ℹ️ No PENDING watermark to rollback")
            return True
                
        except Exception as e:
            self.logger.error(f"Error during rollback: {e}")
            return False
    
    def set_last_extracted_value(self, table_name: str, column_name: str, value: str, metadata: Dict[str, Any] = None) -> bool:
        """Método de interfaz - redirige a save_provisional"""
        self.logger.warning("⚠️ Using set_last_extracted_value - consider using save_provisional + confirm")
        return self.save_provisional(table_name, column_name, value, metadata)
    
    def get_extraction_history(self, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Delega al storage base"""
        return self.base_storage.get_extraction_history(table_name, limit)
    
    def cleanup_old_watermarks(self, days_to_keep: int = 90) -> int:
        """Delega al storage base"""
        return self.base_storage.cleanup_old_watermarks(days_to_keep)