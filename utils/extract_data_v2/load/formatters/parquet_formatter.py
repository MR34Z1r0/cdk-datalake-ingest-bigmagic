# -*- coding: utf-8 -*-
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from typing import Dict, Any, Optional
from exceptions.custom_exceptions import LoadError

class ParquetFormatter:
    """Format data as Parquet files"""
    
    def __init__(self, **kwargs):
        self.compression = kwargs.get('compression', 'snappy')
        self.convert_to_string = kwargs.get('convert_to_string', True)
    
    def format_dataframe(self, df: pd.DataFrame, **kwargs) -> bytes:
        """
        Format DataFrame as Parquet bytes
        
        Args:
            df: DataFrame to format
            **kwargs: Additional formatting options
            
        Returns:
            Parquet data as bytes
        """
        try:
            if df.empty:
                # Create empty DataFrame with at least one column for valid Parquet
                df = pd.DataFrame({'empty_data': []})
            
            # Convert all columns to string if specified
            if self.convert_to_string:
                df_formatted = df.astype(str)
            else:
                df_formatted = df.copy()
            
            # Write to bytes buffer
            buffer = io.BytesIO()
            df_formatted.to_parquet(
                buffer, 
                index=False, 
                engine='pyarrow',
                compression=self.compression
            )
            
            return buffer.getvalue()
            
        except Exception as e:
            raise LoadError(f"Failed to format DataFrame as Parquet: {e}")
    
    def get_file_extension(self) -> str:
        """Get file extension"""
        return '.parquet'
    
    def get_content_type(self) -> str:
        """Get content type for uploads"""
        return 'application/octet-stream'
    
    def validate_dataframe(self, df: pd.DataFrame) -> bool:
        """Validate DataFrame can be converted to Parquet"""
        try:
            if df.empty:
                return True
            
            # Try a small conversion to test
            test_buffer = io.BytesIO()
            df.head(1).to_parquet(test_buffer, index=False, engine='pyarrow')
            return True
            
        except Exception:
            return False