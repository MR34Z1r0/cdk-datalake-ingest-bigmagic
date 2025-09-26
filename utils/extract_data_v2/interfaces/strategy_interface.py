# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from models.table_config import TableConfig
from models.extraction_config import ExtractionConfig
from models.extraction_result import ExtractionResult

class StrategyInterface(ABC):
    """Interface for all extraction strategies"""
    
    def __init__(self, table_config: TableConfig, extraction_config: ExtractionConfig):
        self.table_config = table_config
        self.extraction_config = extraction_config
    
    @abstractmethod
    def generate_queries(self) -> List[Dict[str, Any]]:
        """
        Generate list of queries to execute
        Returns: List of dicts with 'query' and 'metadata' keys
        """
        pass
    
    @abstractmethod
    def get_strategy_name(self) -> str:
        """Get strategy name"""
        pass
    
    @abstractmethod
    def validate_config(self) -> bool:
        """Validate if configuration is valid for this strategy"""
        pass
    
    @abstractmethod
    def estimate_resources(self) -> Dict[str, Any]:
        """Estimate resources needed (threads, memory, etc.)"""
        pass