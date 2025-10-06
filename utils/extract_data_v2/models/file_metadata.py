# utils/extract_data_v2/models/file_metadata.py
# -*- coding: utf-8 -*-
from dataclasses import dataclass, asdict
from typing import Optional
from datetime import datetime
import os

@dataclass
class FileMetadata:
    """Metadata detallada de un archivo generado"""
    file_path: str
    file_name: str
    file_size_bytes: int
    file_size_mb: float
    records_count: int
    thread_id: str
    chunk_id: int
    partition_index: Optional[int]
    created_at: str
    compression: str
    format: str
    
    # Métricas de procesamiento
    extraction_duration_seconds: Optional[float] = None
    upload_duration_seconds: Optional[float] = None
    
    # Información adicional
    columns_count: Optional[int] = None
    estimated_memory_mb: Optional[float] = None
    
    def to_dict(self):
        """Convert to dictionary"""
        return asdict(self)
    
    @staticmethod
    def calculate_file_size_mb(size_bytes: int) -> float:
        """Calculate file size in MB"""
        return round(size_bytes / (1024 * 1024), 2)
    
    @staticmethod
    def estimate_memory_mb(df_shape: tuple) -> float:
        """Estimate memory usage based on DataFrame shape"""
        rows, cols = df_shape
        # Estimación aproximada: 100 bytes por celda
        estimated_bytes = rows * cols * 100
        return round(estimated_bytes / (1024 * 1024), 2)