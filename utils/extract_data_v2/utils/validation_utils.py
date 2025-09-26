# -*- coding: utf-8 -*-
from typing import Dict, Any, List
import re

def validate_required_fields(data: Dict[str, Any], required_fields: List[str]) -> List[str]:
    """Validate that required fields are present and not empty"""
    missing_fields = []
    
    for field in required_fields:
        if field not in data or not data[field] or str(data[field]).strip() == '':
            missing_fields.append(field)
    
    return missing_fields

def validate_db_type(db_type: str) -> bool:
    """Validate database type"""
    valid_types = ['sqlserver', 'mssql', 'postgresql', 'postgres', 'oracle', 'mysql', 'mariadb']
    return db_type.lower() in valid_types

def validate_load_type(load_type: str) -> bool:
    """Validate load type"""
    valid_types = ['full', 'incremental', 'partitioned', 'date_range', 'between-date']
    return load_type.lower() in valid_types

def validate_output_format(output_format: str) -> bool:
    """Validate output format"""
    valid_formats = ['parquet', 'csv', 'json']
    return output_format.lower() in valid_formats

def clean_column_name(column_name: str) -> str:
    """Clean column name removing invalid characters"""
    # Remove quotes and extra spaces
    cleaned = column_name.strip().strip('"\'').strip()
    return cleaned

def validate_sql_identifier(identifier: str) -> bool:
    """Validate SQL identifier (table name, column name, etc.)"""
    # Basic SQL identifier validation
    pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*$'
    return bool(re.match(pattern, identifier))

def sanitize_query_parameter(param: str) -> str:
    """Sanitize query parameter to prevent SQL injection"""
    if param is None:
        return ""
    
    # Remove dangerous characters
    sanitized = str(param).replace("'", "''")  # Escape single quotes
    sanitized = re.sub(r'[;\-\-\/\*]', '', sanitized)  # Remove dangerous SQL chars
    
    return sanitized