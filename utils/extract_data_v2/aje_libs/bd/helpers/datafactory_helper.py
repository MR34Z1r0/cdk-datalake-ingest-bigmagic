from typing import Dict, Any, Optional
from .database.database_helper import DatabaseHelper
from ...common.logger import custom_logger

logger = custom_logger(__name__)

class DatabaseFactoryHelper:
    """Factory class to create appropriate database helper instances."""
    
    @staticmethod
    def create_helper(
        db_type: str,
        server: str,
        database: str,
        username: str,
        password: str,
        port: Optional[int] = None,
        **kwargs
    ) -> DatabaseHelper:
        """
        Create and return an appropriate database helper instance.
        
        :param db_type: Type of database ('sqlserver', 'mysql', 'oracle')
        :param server: Database server address
        :param database: Database name
        :param username: Database username
        :param password: Database password
        :param port: Database port (optional)
        :param kwargs: Additional database-specific parameters
        :return: Database helper instance
        """
        db_type = db_type.lower()
        
        logger.info(f"Creating database helper for {db_type} database: {database} on {server}")
        
        if db_type == 'sqlserver' or db_type == 'mssql':
            driver = kwargs.get('driver', 'SQL Server')
            port = port or 1433
            from .database.sqlserver_helper import SQLServerHelper
            return SQLServerHelper(server, database, username, password, port, driver)
            
        elif db_type == 'mysql' or db_type == 'mariadb':
            charset = kwargs.get('charset', 'utf8mb4')
            port = port or 3306
            from .database.mysql_helper import MySQLHelper
            return MySQLHelper(server, database, username, password, port, charset)
            
        elif db_type == 'oracle':
            service_name = kwargs.get('service_name')
            port = port or 1521
            from .database.oracle_helper import OracleHelper
            return OracleHelper(server, database, username, password, port, service_name)
            
        else:
            raise ValueError(f"Unsupported database type: {db_type}")