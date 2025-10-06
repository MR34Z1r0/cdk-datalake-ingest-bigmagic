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
    files_metadata: Optional[List[Dict[str, Any]]] = None
    
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
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'files_metadata': self.files_metadata
        }
    
    def get_total_size_mb(self) -> float:
        """Get total size of all files in MB"""
        if not self.files_metadata:
            return 0.0
        return sum(f.get('file_size_mb', 0) for f in self.files_metadata)
    
    def get_average_file_size_mb(self) -> float:
        """Get average file size in MB"""
        if not self.files_metadata or len(self.files_metadata) == 0:
            return 0.0
        return self.get_total_size_mb() / len(self.files_metadata)