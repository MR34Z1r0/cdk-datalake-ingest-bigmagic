# -*- coding: utf-8 -*-
import pandas as pd
import io
from typing import Dict, Any, Optional
from exceptions.custom_exceptions import LoadError

class CSVFormatter:
    """Format data as CSV files"""
    
    def __init__(self, **kwargs):
        self.separator = kwargs.get('separator', '|')
        self.quoting = kwargs.get('quoting', 1)  # QUOTE_ALL
        self.encoding = kwargs.get('encoding', 'utf-8')
        self.include_header = kwargs.get('include_header', True)
    
    def format_dataframe(self, df: pd.DataFrame, **kwargs) -> bytes:
        """
        Format DataFrame as CSV bytes
        
        Args:
            df: DataFrame to format
            **kwargs: Additional formatting options
            
        Returns:
            CSV data as bytes
        """
        try:
            if df.empty:
                # For empty DataFrames, create minimal CSV
                if hasattr(df, 'columns') and len(df.columns) > 0:
                    header_line = self.separator.join(df.columns)
                else:
                    header_line = "no_data"
                return header_line.encode(self.encoding)
            
            # Format DataFrame to CSV
            buffer = io.StringIO()
            df.to_csv(
                buffer,
                index=False,
                sep=self.separator,
                quoting=self.quoting,
                header=self.include_header
            )
            
            return buffer.getvalue().encode(self.encoding)
            
        except Exception as e:
            raise LoadError(f"Failed to format DataFrame as CSV: {e}")
    
    def get_file_extension(self) -> str:
        """Get file extension"""
        return '.csv'
    
    def get_content_type(self) -> str:
        """Get content type for uploads"""
        return 'text/csv'
    
    def validate_dataframe(self, df: pd.DataFrame) -> bool:
        """Validate DataFrame can be converted to CSV"""
        try:
            if df.empty:
                return True
            
            # Try a small conversion to test
            test_buffer = io.StringIO()
            df.head(1).to_csv(test_buffer, index=False, sep=self.separator)
            return True
            
        except Exception:
            return False