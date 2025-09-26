# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from typing import Union, Iterator, Optional, Tuple
import pandas as pd
from models.database_config import DatabaseConfig

class ExtractorInterface(ABC):
    """Interface for all data extractors"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
    
    @abstractmethod
    def connect(self):
        """Establish connection to data source"""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test connection to data source"""
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> pd.DataFrame:
        """Execute query and return DataFrame"""
        pass
    
    @abstractmethod
    def execute_query_chunked(self, query: str, chunk_size: int, 
                            order_by: str, params: Optional[Tuple] = None) -> Iterator[pd.DataFrame]:
        """Execute query with chunked results"""
        pass
    
    @abstractmethod
    def get_min_max_values(self, table: str, column: str, 
                          where_clause: Optional[str] = None) -> Tuple[Optional[int], Optional[int]]:
        """Get min and max values for a column"""
        pass
    
    @abstractmethod
    def close(self):
        """Close connection"""
        pass