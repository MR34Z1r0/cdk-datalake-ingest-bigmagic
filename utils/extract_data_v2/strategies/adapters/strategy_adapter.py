# strategies/adapters/strategy_adapter.py
from typing import List, Dict, Any
from interfaces.strategy_interface import StrategyInterface
from ..base.extraction_strategy import ExtractionStrategy
from ..base.extraction_params import ExtractionParams
from aje_libs.common.logger import custom_logger

logger = custom_logger(__name__)

class StrategyAdapter(StrategyInterface):
    """Adaptador para hacer compatible la nueva estrategia con la interfaz existente"""
    
    def __init__(self, new_strategy: ExtractionStrategy):
        self.new_strategy = new_strategy
        self.extraction_params = None
        self.watermark_storage = new_strategy.watermark_storage
    
    def generate_queries(self) -> List[Dict[str, Any]]:
        """Adapta el nuevo método build_extraction_params al formato esperado"""
        logger.info("=== STRATEGY ADAPTER - Generating Queries ===")
        
        # Obtener parámetros de extracción de la nueva estrategia
        self.extraction_params = self.new_strategy.build_extraction_params()
        
        # Construir query SQL
        query = self._build_query_from_params(self.extraction_params)
        
        # Adaptar al formato esperado por el orchestrator
        query_dict = {
            'query': query,
            'thread_id': 0,
            'metadata': {
                'strategy': self.new_strategy.strategy_name,
                'table_name': self.new_strategy.extraction_config.table_name,
                'destination_path': self._build_destination_path(),
                'chunking_params': self._get_chunking_params(),
                **self.extraction_params.metadata
            }
        }
        
        logger.info(f"Generated query for table: {self.extraction_params.table_name}")
        logger.info("=== END STRATEGY ADAPTER ===")
        
        return [query_dict]
    
    def get_strategy_name(self) -> str:
        """Delega al nombre de la nueva estrategia"""
        return self.new_strategy.strategy_name
    
    def validate_config(self) -> bool:
        """Delega a la validación de la nueva estrategia"""
        return self.new_strategy.validate_and_cache()
    
    def estimate_resources(self) -> Dict[str, Any]:
        """Delega a la estimación de recursos de la nueva estrategia"""
        return self.new_strategy.estimate_resources()
    
    def _build_query_from_params(self, params: ExtractionParams) -> str:
        """Construye la query SQL a partir de los parámetros de extracción"""
        
        # SELECT clause
        columns_str = params.get_columns_string()
        
        # FROM clause
        table_name = params.table_name
        
        # WHERE clause
        where_clause = params.get_where_clause()
        
        # Construir query base
        query = f"SELECT {columns_str} FROM {table_name}"
        
        if where_clause:
            query += f" WHERE {where_clause}"
        
        if params.order_by:
            query += f" ORDER BY {params.order_by}"
        
        if params.limit:
            query += f" LIMIT {params.limit}"
        
        return query
    
    def _build_destination_path(self) -> str:
        """Construye el path de destino S3"""
        from utils.date_utils import get_date_parts
        
        year, month, day = get_date_parts()
        
        # Obtener nombre de tabla limpio
        clean_table_name = self._get_clean_table_name()
        
        return (f"{self.new_strategy.extraction_config.team}/"
                f"{self.new_strategy.extraction_config.data_source}/"
                f"{self.new_strategy.extraction_config.endpoint_name}/"
                f"{clean_table_name}/year={year}/month={month}/day={day}/")
    
    def _get_clean_table_name(self) -> str:
        """Extrae nombre de tabla limpio"""
        source_table = (self.new_strategy.table_config.source_table or 
                       self.new_strategy.extraction_config.table_name)
        
        if source_table and ' ' in source_table:
            return source_table.split()[0]
        return source_table
    
    def _get_chunking_params(self) -> Dict[str, Any]:
        """Obtiene parámetros de chunking"""
        if not self.extraction_params:
            return {}
        
        chunking_params = {}
        
        if self.extraction_params.chunk_size:
            chunking_params['chunk_size'] = self.extraction_params.chunk_size
        
        if self.extraction_params.chunk_column:
            chunking_params['order_by'] = self.extraction_params.chunk_column
        
        return chunking_params