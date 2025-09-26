# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from datetime import datetime

@dataclass
class ExtractionResult:
    """Result of extraction operation"""
    success: bool
    table_name: str
    records_extracted: int
    files_created: List[str]
    execution_time_seconds: float
    strategy_used: str
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging"""
        return {
            'success': self.success,
            'table_name': self.table_name,
            'records_extracted': self.records_extracted,
            'files_created': self.files_created,
            'execution_time_seconds': self.execution_time_seconds,
            'strategy_used': self.strategy_used,
            'error_message': self.error_message,
            'metadata': self.metadata,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None
        }