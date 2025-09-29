# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from typing import Dict, Any
from models.extraction_result import ExtractionResult

class MonitorInterface(ABC):
    """Interface for all monitoring systems"""
    
    @abstractmethod
    def log_start(self, table_name: str, strategy: str, metadata: Dict[str, Any] = None):
        """
        Log extraction start
        Returns: process_id
        """
        pass
    
    @abstractmethod
    def log_success(self, result: ExtractionResult):
        """
        Log successful extraction
        Returns: process_id
        """
        pass
    
    @abstractmethod
    def log_error(self, table_name: str, error_message: str, metadata: Dict[str, Any] = None):
        """
        Log extraction error
        Returns: process_id
        """
        pass
    
    @abstractmethod
    def log_warning(self, table_name: str, warning_message: str, metadata: Dict[str, Any] = None) -> str:
        """
        Log extraction warning
        Returns: process_id
        """
        pass
    
    @abstractmethod
    def send_notification(self, message: str, is_error: bool = False):
        """Send notification"""
        pass