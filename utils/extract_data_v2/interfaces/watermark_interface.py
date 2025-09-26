# interfaces/watermark_interface.py
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from datetime import datetime

class WatermarkStorageInterface(ABC):
    """Interface para almacenamiento de watermarks/valores de extracción"""
    
    @abstractmethod
    def get_last_extracted_value(self, table_name: str, column_name: str) -> Optional[str]:
        """Obtiene el último valor extraído para una tabla/columna"""
        pass
    
    @abstractmethod
    def set_last_extracted_value(self, table_name: str, column_name: str, 
                               value: str, metadata: Dict[str, Any] = None) -> bool:
        """Guarda el último valor extraído"""
        pass
    
    @abstractmethod
    def get_extraction_history(self, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Obtiene el historial de extracciones"""
        pass
    
    @abstractmethod
    def cleanup_old_watermarks(self, days_to_keep: int = 90) -> int:
        """Limpia watermarks antiguos"""
        pass