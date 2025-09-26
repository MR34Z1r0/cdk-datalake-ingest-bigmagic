# strategies/base/extraction_params.py
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

@dataclass
class ExtractionParams:
    """Parámetros unificados para todas las estrategias de extracción"""
    
    # Básicos
    table_name: str
    columns: List[str]
    
    # Filtros
    where_conditions: List[str] = field(default_factory=list)
    
    # Ordenamiento y paginación
    order_by: Optional[str] = None
    limit: Optional[int] = None
    
    # Chunking
    chunk_size: Optional[int] = None
    chunk_column: Optional[str] = None
    
    # Metadatos
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validaciones básicas después de inicialización"""
        if not self.table_name:
            raise ValueError("table_name es requerido")
        
        if not self.columns:
            self.columns = ['*']
        
        # Limpiar condiciones vacías
        self.where_conditions = [cond for cond in self.where_conditions if cond and cond.strip()]
    
    def add_where_condition(self, condition: str):
        """Agrega una condición WHERE"""
        if condition and condition.strip():
            self.where_conditions.append(condition.strip())
    
    def get_where_clause(self) -> Optional[str]:
        """Retorna la cláusula WHERE completa o None"""
        if not self.where_conditions:
            return None
        return " AND ".join(self.where_conditions)
    
    def get_columns_string(self) -> str:
        """Retorna las columnas como string para SQL"""
        if not self.columns or self.columns == ['*']:
            return '*'
        return ', '.join(self.columns)