# utils/extract_data_v2/core/partition_formatter.py
from datetime import datetime
from typing import Dict, Optional
import pytz
import re

class PartitionFormatter:
    """Formatea rutas de partición basadas en plantillas configurables"""
    
    DEFAULT_FORMAT = "year={YYYY}/month={MM}/day={DD}"
    TZ_LIMA = pytz.timezone('America/Lima')
    
    # Mapeo de tokens a formatos strftime
    TOKEN_MAPPING = {
        '{YYYY}': '%Y',
        '{YY}': '%y',
        '{MM}': '%m',
        '{MON}': '%b',
        '{DD}': '%d',
        '{HH}': '%H',
        '{MI}': '%M',
        '{SS}': '%S',
        '{WEEK}': '%W',
        '{QUARTER}': None  # Manejado especialmente
    }
    
    def __init__(self, format_template: Optional[str] = None):
        """
        Inicializa el formateador con una plantilla
        
        Args:
            format_template: Plantilla de formato o None para usar default
        """
        self.format_template = format_template or self.DEFAULT_FORMAT
        self.validate_format()
    
    def validate_format(self) -> None:
        """Valida que el formato contenga tokens válidos"""
        # Extraer todos los tokens del formato
        tokens = re.findall(r'\{[^}]+\}', self.format_template)
        
        for token in tokens:
            if token not in self.TOKEN_MAPPING:
                raise ValueError(f"Token no válido en formato de partición: {token}")
    
    def format_path(self, timestamp: Optional[datetime] = None) -> str:
        """
        Formatea la ruta de partición basada en la plantilla
        
        Args:
            timestamp: Timestamp a usar, si es None usa el actual
            
        Returns:
            Ruta formateada según la plantilla
        """
        if timestamp is None:
            timestamp = datetime.now(self.TZ_LIMA)
        elif timestamp.tzinfo is None:
            timestamp = self.TZ_LIMA.localize(timestamp)
        
        formatted_path = self.format_template
        
        for token, strftime_format in self.TOKEN_MAPPING.items():
            if token in formatted_path:
                if token == '{QUARTER}':
                    # Manejo especial para trimestres
                    quarter = (timestamp.month - 1) // 3 + 1
                    value = f"Q{quarter}"
                else:
                    value = timestamp.strftime(strftime_format)
                
                formatted_path = formatted_path.replace(token, value)
        
        return formatted_path
    
    def extract_partition_values(self, path: str) -> Dict[str, str]:
        """
        Extrae valores de partición de una ruta existente
        
        Args:
            path: Ruta con particiones
            
        Returns:
            Diccionario con nombres y valores de particiones
        """
        partition_values = {}
        
        # Buscar patrones tipo key=value
        matches = re.findall(r'(\w+)=([^/]+)', path)
        
        for key, value in matches:
            partition_values[key] = value
        
        return partition_values
    
    @classmethod
    def parse_partition_path(cls, path: str) -> Dict[str, str]:
        """
        Parsea una ruta de partición y retorna sus componentes
        
        Args:
            path: Ruta con formato de partición
            
        Returns:
            Diccionario con los componentes de la partición
        """
        formatter = cls()
        return formatter.extract_partition_values(path)