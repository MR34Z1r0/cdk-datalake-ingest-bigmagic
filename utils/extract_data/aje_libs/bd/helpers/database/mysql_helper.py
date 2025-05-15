import pymysql
from typing import List, Dict, Any, Optional, Union, Tuple
from .database_helper import DatabaseHelper
from ....common.logger import custom_logger

logger = custom_logger(__name__)

class MySQLHelper(DatabaseHelper):
    """Helper for MySQL/MariaDB database operations."""

    def __init__(
        self,
        server: str,
        database: str,
        username: str,
        password: str,
        port: int = 3306,
        charset: str = 'utf8mb4'
    ) -> None:
        """
        Initialize the MySQL helper.

        :param server: MySQL server hostname/address.
        :param database: Database name.
        :param username: MySQL username.
        :param password: MySQL password.
        :param port: MySQL port (default: 3306).
        :param charset: Character set (default: utf8mb4).
        """
        super().__init__(server, database, username, password, port)
        self.charset = charset
        logger.info(f"Configured helper for MySQL database: {database} on {server}")

    def connect(self) -> pymysql.connections.Connection:
        """
        Establish a connection to the MySQL database.
        
        :return: Database connection object.
        """
        try:
            conn = pymysql.connect(
                host=self.server,
                user=self.username,
                password=self.password,
                database=self.database,
                port=self.port,
                charset=self.charset,
                cursorclass=pymysql.cursors.DictCursor
            )
            logger.debug("MySQL connection established successfully")
            return conn
        except Exception as e:
            logger.error(f"Error connecting to MySQL: {e}")
            raise

    def execute_query(self, query: str, params: Optional[Union[Tuple, Dict]] = None) -> List[Tuple]:
        """
        Execute a query and return all results.
        
        :param query: SQL query to execute.
        :param params: Parameters for the query (optional).
        :return: List of result rows as tuples.
        """
        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                
            logger.info(f"Query executed successfully, returned {len(results)} rows")
            
            # Convert dict results to tuples to match interface
            tuple_results = []
            if results and isinstance(results[0], dict):
                for row in results:
                    tuple_results.append(tuple(row.values()))
                return tuple_results
            
            return results
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
        finally:
            conn.close()

    def execute_query_as_dict(self, query: str, params: Optional[Union[Tuple, Dict]] = None) -> List[Dict[str, Any]]:
        """
        Execute a query and return results as dictionaries.
        
        :param query: SQL query to execute.
        :param params: Parameters for the query (optional).
        :return: List of result rows as dictionaries.
        """
        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                
            logger.info(f"Query executed successfully, returned {len(results)} rows as dictionaries")
            return results
        except Exception as e:
            logger.error(f"Error executing query as dict: {e}")
            raise
        finally:
            conn.close()

    def execute_non_query(self, query: str, params: Optional[Union[Tuple, Dict]] = None) -> int:
        """
        Execute a non-query statement (INSERT, UPDATE, DELETE).
        
        :param query: SQL statement to execute.
        :param params: Parameters for the statement (optional).
        :return: Number of affected rows.
        """
        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                affected_rows = cursor.execute(query, params)
                
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
            with conn.cursor() as cursor:
                if params:
                    # Format the parameter string for MySQL
                    param_string = ", ".join(["%s"] * len(params))
                    cursor.execute(f"CALL {proc_name}({param_string})", list(params.values()))
                else:
                    cursor.execute(f"CALL {proc_name}()")
                
                results = cursor.fetchall()
                
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
            with conn.cursor() as cursor:
                # Build the INSERT statement
                placeholders = ",".join(["%s"] * len(columns))
                column_names = ",".join([f"`{col}`" for col in columns])
                insert_query = f"INSERT INTO `{table_name}` ({column_names}) VALUES ({placeholders})"
                
                # Execute batch insert
                row_count = cursor.executemany(insert_query, data)
                
                conn.commit()
                logger.info(f"Batch insert completed, inserted {row_count} rows into {table_name}")
                return row_count
        except Exception as e:
            conn.rollback()
            logger.error(f"Error performing batch insert: {e}")
            raise
        finally:
            conn.close()