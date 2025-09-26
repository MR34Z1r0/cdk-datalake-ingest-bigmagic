# strategies/base/strategy_types.py
from enum import Enum

class ExtractionStrategyType(Enum):
    """Tipos de estrategias de extracción disponibles"""
    FULL_LOAD = "full"
    INCREMENTAL = "incremental"
    TIME_RANGE = "time_range"
    
    @classmethod
    def from_string(cls, value: str):
        """Convierte string a enum, manejando aliases"""
        value = value.lower().strip()
        
        # Mapeo de aliases
        alias_map = {
            'full': cls.FULL_LOAD,
            'incremental': cls.INCREMENTAL,
            'time_range': cls.TIME_RANGE,
            'date_range': cls.TIME_RANGE,
            'between-date': cls.TIME_RANGE,
        }
        
        if value in alias_map:
            return alias_map[value]
        
        # Buscar por valor directo
        for strategy_type in cls:
            if strategy_type.value == value:
                return strategy_type
        
        raise ValueError(f"Tipo de estrategia no reconocido: {value}")