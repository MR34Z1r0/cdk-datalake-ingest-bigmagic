import pyodbc
from typing import List, Dict, Any, Optional, Union, Tuple
from .database_helper import DatabaseHelper
from ....common.logger import custom_logger

logger = custom_logger(__name__)

class SQLServerHelper(DatabaseHelper):
    """Custom helper for SQL Server to simplify database operations."""

    def __init__(
        self,
        server: str,
        database: str,
        username: str,
        password: str,
        port: Optional[int] = 1433,
        driver: str = "SQL Server"
    ) -> None:
        """
        Initialize the SQL Server helper.

        :param server: SQL Server hostname/address.
        :param database: Database name.
        :param username: SQL Server username.
        :param password: SQL Server password.
        :param port: SQL Server port (default: 1433).
        :param driver: ODBC driver name (default: "SQL Server").
        """
        super().__init__(server, database, username, password, port)
        self.driver = driver
        self.connection_string = (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password}"
        )
        if port:
            self.connection_string += f";PORT={port}"
            
        logger.info(f"Configured helper for SQL Server database: {database} on {server}")

    def connect(self) -> pyodbc.Connection:
        """
        Establish a connection to the SQL Server database.
        
        :return: Database connection object.
        """
        try:
            conn = pyodbc.connect(self.connection_string)
            logger.debug("SQL Server connection established successfully")
            return conn
        except Exception as e:
            logger.error(f"Error connecting to SQL Server: {e}")
            raise

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """
        Execute a query and return all results.
        
        :param query: SQL query to execute.
        :param params: Parameters for the query (optional).
        :return: List of result rows as tuples.
        """
        conn = self.connect()
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            results = cursor.fetchall()
            logger.info(f"Query executed successfully, returned {len(results)} rows")
            return results
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
        finally:
            conn.close()

    def execute_query_as_dict(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a query and return results as dictionaries.
        
        :param query: SQL query to execute.
        :param params: Parameters for the query (optional).
        :return: List of result rows as dictionaries.
        """
        conn = self.connect()
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            columns = [column[0] for column in cursor.description]
            results = []
            
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            logger.info(f"Query executed successfully, returned {len(results)} rows as dictionaries")
            return results
        except Exception as e:
            logger.error(f"Error executing query as dict: {e}")
            raise
        finally:
            conn.close()

    def execute_non_query(self, query: str, params: Optional[Tuple] = None) -> int:
        """
        Execute a non-query statement (INSERT, UPDATE, DELETE).
        
        :param query: SQL statement to execute.
        :param params: Parameters for the statement (optional).
        :return: Number of affected rows.
        """
        conn = self.connect()
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            affected_rows = cursor.rowcount
            conn.commit()
            logger.info(f"Non-query executed successfully, affected {affected_rows} rows")
            return affected_rows
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing non-query: {e}")
            raise
        finally:
            conn.close()
            
    def execute_stored_procedure(self, proc_name: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a stored procedure and return results as dictionaries.
        
        :param proc_name: Name of the stored procedure.
        :param params: Dictionary of parameters for the procedure (optional).
        :return: List of result rows as dictionaries.
        """
        conn = self.connect()
        try:
            cursor = conn.cursor()
            
            if params:
                # Build the parameter string
                param_string = ""
                param_values = []
                
                for key, value in params.items():
                    if param_string:
                        param_string += ", "
                    param_string += f"@{key}=?"
                    param_values.append(value)
                
                cursor.execute(f"EXEC {proc_name} {param_string}", param_values)
            else:
                cursor.execute(f"EXEC {proc_name}")
            
            # Process results
            results = []
            if cursor.description:
                columns = [column[0] for column in cursor.description]
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))
            
            logger.info(f"Stored procedure {proc_name} executed successfully")
            return results
        except Exception as e:
            logger.error(f"Error executing stored procedure {proc_name}: {e}")
            raise
        finally:
            conn.close()
            
    def batch_insert(self, table_name: str, columns: List[str], data: List[List[Any]]) -> int:
        """
        Perform a batch insert operation.
        
        :param table_name: Target table name.
        :param columns: List of column names.
        :param data: List of rows, where each row is a list of values.
        :return: Number of inserted rows.
        """
        if not data:
            logger.warning("No data to insert")
            return 0
            
        conn = self.connect()
        try:
            cursor = conn.cursor()
            
            # Build the INSERT statement
            placeholders = ",".join(["?"] * len(columns))
            column_names = ",".join(columns)
            insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
            
            # Execute batch insert
            row_count = 0
            for row in data:
                cursor.execute(insert_query, row)
                row_count += 1
                
                # Commit every 1000 rows to avoid transaction log growth
                if row_count % 1000 == 0:
                    conn.commit()
                    logger.debug(f"Committed {row_count} rows so far")
            
            conn.commit()
            logger.info(f"Batch insert completed, inserted {row_count} rows into {table_name}")
            return row_count
        except Exception as e:
            conn.rollback()
            logger.error(f"Error performing batch insert: {e}")
            raise
        finally:
            conn.close()