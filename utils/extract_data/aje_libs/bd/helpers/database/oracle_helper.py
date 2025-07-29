# src/aje_libs/common/helpers/oracle_helper.py
import cx_Oracle
from typing import List, Dict, Any, Optional, Union, Tuple
from .database_helper import DatabaseHelper
from ....common.logger import custom_logger

logger = custom_logger(__name__)

class OracleHelper(DatabaseHelper):
    """Helper for Oracle database operations."""

    def __init__(
        self,
        server: str,
        database: str,
        username: str,
        password: str,
        port: int = 1521,
        service_name: Optional[str] = None
    ) -> None:
        """
        Initialize the Oracle helper.

        :param server: Oracle server hostname/address.
        :param database: Database SID or service name.
        :param username: Oracle username.
        :param password: Oracle password.
        :param port: Oracle port (default: 1521).
        :param service_name: Oracle service name (if using service name instead of SID).
        """
        super().__init__(server, database, username, password, port)
        self.service_name = service_name
        
        # Build connection string
        if service_name:
            self.dsn = cx_Oracle.makedsn(server, port, service_name=service_name)
        else:
            self.dsn = cx_Oracle.makedsn(server, port, sid=database)
            
        logger.info(f"Configured helper for Oracle database: {database} on {server}")

    def connect(self) -> cx_Oracle.Connection:
        """
        Establish a connection to the Oracle database.
        
        :return: Database connection object.
        """
        try:
            conn = cx_Oracle.connect(
                user=self.username,
                password=self.password,
                dsn=self.dsn
            )
            logger.debug("Oracle connection established successfully")
            return conn
        except Exception as e:
            logger.error(f"Error connecting to Oracle: {e}")
            raise

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Tuple]:
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
            cursor.close()
            conn.close()

    def execute_query_as_dict(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
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
            
            # Get column names
            columns = [col[0] for col in cursor.description]
            
            # Convert results to dictionaries
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
                
            logger.info(f"Query executed successfully, returned {len(results)} rows as dictionaries")
            return results
        except Exception as e:
            logger.error(f"Error executing query as dict: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def execute_non_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> int:
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
            cursor.close()
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
                # Build the call with named parameters
                param_names = list(params.keys())
                param_vars = {f":{name}": value for name, value in params.items()}
                
                # Build the procedure call with bind variables
                param_string = ", ".join([f":{name}" for name in param_names])
                call_statement = f"BEGIN {proc_name}({param_string}); END;"
                
                cursor.execute(call_statement, param_vars)
            else:
                cursor.execute(f"BEGIN {proc_name}; END;")
            
            # If there's a cursor returned, fetch the results
            results = []
            if cursor.description:
                columns = [col[0] for col in cursor.description]
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))
            
            logger.info(f"Stored procedure {proc_name} executed successfully")
            return results
        except Exception as e:
            logger.error(f"Error executing stored procedure {proc_name}: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
            
    def batch_insert(self, table_name: str, columns: List[str], data: List[List[Any]]) -> int:
        """
        Perform a batch insert operation using array interface.
        
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
            column_names = ", ".join(columns)
            placeholders = ", ".join([f":{i+1}" for i in range(len(columns))])
            insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
            
            # Execute batch insert
            cursor.executemany(insert_query, data)
            row_count = cursor.rowcount
            
            conn.commit()
            logger.info(f"Batch insert completed, inserted {row_count} rows into {table_name}")
            return row_count
        except Exception as e:
            conn.rollback()
            logger.error(f"Error performing batch insert: {e}")
            raise
        finally:
            cursor.close()
            conn.close()