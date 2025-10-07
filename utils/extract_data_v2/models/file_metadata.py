from decimal import Decimal
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
import json

@dataclass
class FileMetadata:
    file_path: str
    file_name: str
    file_size_bytes: int
    file_size_mb: Decimal  # Cambiar a Decimal
    records_count: int
    thread_id: str
    chunk_id: int
    partition_index: Optional[int]
    created_at: str
    compression: str
    format: str
    extraction_duration_seconds: Decimal  # Cambiar a Decimal
    upload_duration_seconds: Decimal  # Cambiar a Decimal
    columns_count: int
    estimated_memory_mb: Decimal  # Cambiar a Decimal
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        return data
    
    @staticmethod
    def calculate_file_size_mb(size_bytes: int) -> Decimal:
        return Decimal(str(round(size_bytes / (1024 * 1024), 3)))
    
    @staticmethod
    def estimate_memory_mb(shape: tuple) -> Decimal:
        rows, cols = shape
        return Decimal(str(round((rows * cols * 8) / (1024 * 1024), 3)))