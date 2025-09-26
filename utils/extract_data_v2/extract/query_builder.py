# -*- coding: utf-8 -*-
from typing import Optional, Dict, Any
import re
from models.table_config import TableConfig
from utils.validation_utils import clean_column_name, sanitize_query_parameter

class QueryBuilder:
    """Build SQL queries based on table configuration"""
    
    def __init__(self, table_config: TableConfig):
        self.table_config = table_config
    
    def build_standard_query(self, additional_where: Optional[str] = None) -> str:
        """Build standard SELECT query"""
        columns = self._process_columns()
        
        query = f"SELECT {columns} FROM {self.table_config.source_schema}.{self.table_config.source_table}"
        
        if self.table_config.join_expr:
            query += f" {self.table_config.join_expr}"
        
        where_conditions = []
        
        # Add filter expression if exists
        if self.table_config.filter_exp and self.table_config.filter_exp.strip():
            clean_filter = self.table_config.filter_exp.replace('"', '')
            where_conditions.append(f"({clean_filter})")
        
        # Add additional where clause
        if additional_where:
            where_conditions.append(f"({additional_where})")
        
        if where_conditions:
            query += f" WHERE {' AND '.join(where_conditions)}"
        
        return query
    
    def build_partitioned_query(self, partition_column: str, start_value: int, 
                               end_value: int, additional_where: Optional[str] = None) -> str:
        """Build partitioned query with range"""
        columns = self._process_columns()
        
        query = f"SELECT {columns} FROM {self.table_config.source_schema}.{self.table_config.source_table}"
        
        if self.table_config.join_expr:
            query += f" {self.table_config.join_expr}"
        
        where_conditions = []
        
        # Add partition range
        where_conditions.append(f"{partition_column} >= {start_value} AND {partition_column} < {end_value}")
        
        # Add filter expression if exists
        if self.table_config.filter_exp and self.table_config.filter_exp.strip():
            clean_filter = self.table_config.filter_exp.replace('"', '')
            where_conditions.append(f"({clean_filter})")
        
        # Add additional where clause
        if additional_where:
            where_conditions.append(f"({additional_where})")
        
        if where_conditions:
            query += f" WHERE {' AND '.join(where_conditions)}"
        
        return query
    
    def build_date_range_query(self, start_date: str, end_date: str, 
                              date_column: str, date_type: str = None) -> str:
        """Build date range query"""
        from utils.date_utils import format_date_for_db
        
        columns = self._process_columns()
        
        query = f"SELECT {columns} FROM {self.table_config.source_schema}.{self.table_config.source_table}"
        
        if self.table_config.join_expr:
            query += f" {self.table_config.join_expr}"
        
        where_conditions = []
        
        # Format dates based on database type
        if date_type:
            formatted_start = format_date_for_db(start_date, date_type)
            formatted_end = format_date_for_db(end_date, date_type)
        else:
            formatted_start = f"'{start_date}'"
            formatted_end = f"'{end_date}'"
        
        # Handle multiple date columns
        if ',' in date_column:
            date_columns = [col.strip() for col in date_column.split(',')]
            date_conditions = []
            for col in date_columns:
                date_conditions.append(f"({col} IS NOT NULL AND {col} BETWEEN {formatted_start} AND {formatted_end})")
            where_conditions.append(f"({' OR '.join(date_conditions)})")
        else:
            where_conditions.append(f"{date_column} IS NOT NULL AND {date_column} BETWEEN {formatted_start} AND {formatted_end}")
        
        # Add filter expression if exists
        if self.table_config.filter_exp and self.table_config.filter_exp.strip():
            clean_filter = self.table_config.filter_exp.replace('"', '')
            where_conditions.append(f"({clean_filter})")
        
        if where_conditions:
            query += f" WHERE {' AND '.join(where_conditions)}"
        
        return query
    
    def build_min_max_query(self, column: str, additional_where: Optional[str] = None) -> str:
        """Build query to get min and max values"""
        query = f"SELECT MIN({column}) as min_val, MAX({column}) as max_val FROM {self.table_config.source_schema}.{self.table_config.source_table}"
        
        if self.table_config.join_expr:
            query += f" {self.table_config.join_expr}"
        
        where_conditions = [f"{column} <> 0"]
        
        # Add filter expression if exists
        if self.table_config.filter_exp and self.table_config.filter_exp.strip():
            clean_filter = self.table_config.filter_exp.replace('"', '')
            where_conditions.append(f"({clean_filter})")
        
        # Add additional where clause
        if additional_where:
            where_conditions.append(f"({additional_where})")
        
        if where_conditions:
            query += f" WHERE {' AND '.join(where_conditions)}"
        
        return query
    
    def _process_columns(self) -> str:
        """Process and clean column definitions"""
        columns_str = self.table_config.columns
        
        # Add ID column if specified
        if self.table_config.id_column and self.table_config.id_column.strip():
            columns_str = f"{self.table_config.id_column} as id, {columns_str}"
        
        # Remove problematic double quotes
        columns_str = self._clean_column_quotes(columns_str)
        
        # Process individual columns
        processed_columns = self._split_and_clean_columns(columns_str)
        
        return ', '.join(processed_columns)
    
    def _clean_column_quotes(self, columns_str: str) -> str:
        """Remove problematic double quotes from column string"""
        if not columns_str:
            return columns_str
        
        clean_columns = columns_str.strip()
        
        # Check for double quotes and remove them
        double_quote_count = clean_columns.count('"')
        if double_quote_count > 0:
            # If entire field is wrapped in quotes, remove them
            if clean_columns.startswith('"') and clean_columns.endswith('"') and double_quote_count == 2:
                clean_columns = clean_columns[1:-1]
            else:
                # Remove all double quotes
                clean_columns = clean_columns.replace('"', '')
        
        return clean_columns
    
    def _split_and_clean_columns(self, columns_str: str) -> list:
        """Split columns intelligently and clean them"""
        if not columns_str or columns_str.strip() == '':
            return ['*']
        
        columns = []
        current_column = ""
        paren_count = 0
        in_single_quote = False
        
        i = 0
        while i < len(columns_str):
            char = columns_str[i]
            current_column += char
            
            if char == "'" and not in_single_quote:
                in_single_quote = True
            elif char == "'" and in_single_quote:
                in_single_quote = False
            elif not in_single_quote:
                if char == '(':
                    paren_count += 1
                elif char == ')':
                    paren_count -= 1
                elif char == ',' and paren_count == 0:
                    # This is a column separator
                    column_text = current_column[:-1].strip()  # Remove the comma
                    if column_text:
                        columns.append(column_text)
                    current_column = ""
            i += 1
        
        # Add the last column
        if current_column.strip():
            columns.append(current_column.strip())
        
        return [col for col in columns if col.strip()]
    
    def extract_columns_from_query(self, query: str) -> list:
        """Extract column names from SELECT statement"""
        try:
            # Remove extra whitespaces
            clean_query = ' '.join(query.strip().split())
            
            # Find SELECT and FROM positions
            select_match = re.search(r'\bSELECT\s+', clean_query, re.IGNORECASE)
            from_match = re.search(r'\bFROM\s+', clean_query, re.IGNORECASE)
            
            if not select_match or not from_match:
                return ['unknown_column']
            
            # Extract the column part
            select_start = select_match.end()
            from_start = from_match.start()
            columns_part = clean_query[select_start:from_start].strip()
            
            # Split columns intelligently
            column_expressions = self._split_and_clean_columns(columns_part)
            
            columns = []
            for expr in column_expressions:
                column_name = self._extract_column_alias_or_name(expr.strip())
                if column_name:
                    columns.append(column_name)
            
            return columns or ['unknown_column']
            
        except Exception:
            return ['unknown_column']
    
    def _extract_column_alias_or_name(self, expression: str) -> str:
        """Extract column alias or name from expression"""
        try:
            expr = expression.strip()
            
            # Check for explicit AS alias
            as_match = re.search(r'\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*$', expr, re.IGNORECASE)
            if as_match:
                return as_match.group(1).strip()
            
            # Check for implicit alias for complex expressions
            if any(indicator in expr.lower() for indicator in ['(', '+', '-', '*', '/', 'ltrim', 'rtrim', 'convert', 'cast']):
                words = expr.split()
                if len(words) >= 2:
                    potential_alias = words[-1].strip()
                    if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', potential_alias):
                        excluded_keywords = ['and', 'or', 'not', 'in', 'like', 'is', 'null', 'from', 'where', 'select']
                        if potential_alias.lower() not in excluded_keywords:
                            return potential_alias
            
            # Handle table.column format
            table_column_match = re.match(r'^([a-zA-Z_][a-zA-Z0-9_]*)\\.([a-zA-Z_][a-zA-Z0-9_]*)$', expr.strip())
            if table_column_match:
                return table_column_match.group(2)
            
            # Simple column name
            if not any(indicator in expr.lower() for indicator in ['(', '+', '-', '*', '/', 'ltrim', 'rtrim', 'convert', 'cast', "'", '"']):
                clean_name = expr.strip('[]"`').strip()
                if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', clean_name):
                    return clean_name
            
            # Fallback
            return f'expr_field_{hash(expr) % 1000}'
            
        except Exception:
            return f'unknown_col_{hash(expression) % 1000}'