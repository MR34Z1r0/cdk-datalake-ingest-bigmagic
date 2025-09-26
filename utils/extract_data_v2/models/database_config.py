# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Optional

@dataclass
class DatabaseConfig:
    """Database configuration model"""
    endpoint_name: str
    db_type: str  # 'sqlserver', 'postgresql', 'oracle', 'mysql'
    server: str
    database: str
    username: str
    secret_key: str  # Key to get password from secrets
    secret_name: str
    port: Optional[int] = None
    
    def __post_init__(self):
        """Validate required fields"""
        if not all([self.endpoint_name, self.db_type, self.server, 
                   self.database, self.username, self.secret_key]):
            raise ValueError("All database configuration fields are required")