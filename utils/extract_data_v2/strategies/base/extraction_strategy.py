# strategies/base/extraction_strategy.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from .extraction_params import ExtractionParams
from .strategy_types import ExtractionStrategyType
from models.table_config import TableConfig
from models.extraction_config import ExtractionConfig
from interfaces.watermark_interface import WatermarkStorageInterface

class ExtractionStrategy(ABC):
    """Estrategia base simplificada para extracción de datos"""
    
    def __init__(self, table_config: TableConfig, extraction_config: ExtractionConfig, watermark_storage: WatermarkStorageInterface = None):
        self.table_config = table_config
        self.extraction_config = extraction_config
        self.watermark_storage = watermark_storage
        self._validated = False
    
    @abstractmethod
    def build_extraction_params(self) -> ExtractionParams:
        """Construye los parámetros de extracción específicos para esta estrategia"""
        pass
    
    @abstractmethod
    def validate(self) -> bool:
        """Valida que la estrategia pueda ejecutarse con la configuración actual"""
        pass
    
    @abstractmethod
    def get_strategy_type(self) -> ExtractionStrategyType:
        """Retorna el tipo de estrategia"""
        pass
    
    @property
    def strategy_name(self) -> str:
        """Nombre de la estrategia para logging"""
        return self.__class__.__name__.replace('Strategy', '').lower()
    
    def validate_and_cache(self) -> bool:
        """Valida y cachea el resultado"""
        if not self._validated:
            self._validated = self.validate()
        return self._validated
    
    def estimate_resources(self) -> Dict[str, Any]:
        """Estima recursos necesarios (implementación por defecto)"""
        return {
            'estimated_threads': 1,
            'estimated_memory_mb': 500,
            'supports_chunking': False,
            'parallel_safe': True
        }
    
    # Métodos helper comunes
    def _parse_columns(self) -> List[str]:
        """Parse column string into list"""
        if not self.table_config.columns or self.table_config.columns.strip() == '':
            return ['*']
        
        columns = []
        for col in self.table_config.columns.split(','):
            clean_col = col.strip()
            if clean_col:
                columns.append(clean_col)
        
        return columns if columns else ['*']
    
    def _get_source_table_name(self) -> str:
        """Obtiene el nombre de la tabla fuente limpio"""
        source_table = self.table_config.source_table or self.extraction_config.table_name
        # Remover alias (texto después del primer espacio)
        if source_table and ' ' in source_table:
            return source_table.split()[0]
        return source_table
    
    def _build_basic_metadata(self) -> Dict[str, Any]:
        """Construye metadatos básicos para la extracción"""
        return {
            'strategy': self.strategy_name,
            'table_name': self.extraction_config.table_name,
            'source_table': self._get_source_table_name(),
            'load_type': self.table_config.load_type,
            'timestamp': self.extraction_config.execution_timestamp
        }