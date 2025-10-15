# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Optional

@dataclass
class TableConfig:
    """Table configuration model"""
    stage_table_name: str
    source_schema: str
    source_table: str
    columns: str
    load_type: str  # 'full', 'incremental', 'partitioned', 'date_range'
    source_table_type: str
    partition_mode: Optional[str] = None
    id_column: Optional[str] = None
    partition_column: Optional[str] = None
    filter_exp: Optional[str] = None
    filter_column: Optional[str] = None
    filter_data_type: Optional[str] = None
    join_expr: Optional[str] = None
    delay_incremental_ini: Optional[str] = None
    delay_incremental_end: Optional[str] = None  # 👈 NUEVO CAMPO
    start_value: Optional[str] = None
    end_value: Optional[str] = None
    
    def __post_init__(self):
        """Validate required fields"""
        if not all([self.stage_table_name, self.source_schema, 
                   self.source_table, self.columns, self.load_type]):
            raise ValueError("Essential table configuration fields are required")