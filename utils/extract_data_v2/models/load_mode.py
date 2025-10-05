# models/load_mode.py
from enum import Enum

class LoadMode(Enum):
    """
    Modos de ejecución de carga
    
    - INITIAL: Primera carga de la tabla (carga completa + guarda watermark)
    - NORMAL: Carga frecuente/regular (incremental desde watermark o full sin watermark)
    - RESET: Reiniciar desde cero (limpia watermark + carga completa + guarda nuevo watermark)
    - REPROCESS: Reprocesar con filtros específicos (ej: rango de fechas)
    """
    INITIAL = "initial"
    NORMAL = "normal"
    RESET = "reset"
    REPROCESS = "reprocess"
    
    @classmethod
    def from_string(cls, value: str) -> 'LoadMode':
        """Convierte string a enum"""
        value = value.lower().strip()
        
        for mode in cls:
            if mode.value == value:
                return mode
        
        raise ValueError(f"Load mode no reconocido: {value}. Valores válidos: {[m.value for m in cls]}")