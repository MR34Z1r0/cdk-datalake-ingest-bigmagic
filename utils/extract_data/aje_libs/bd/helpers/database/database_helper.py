from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Dict, Any, Optional, Union, Tuple
from ....common.logger import custom_logger

logger = custom_logger(__name__)

class DatabaseHelper(ABC):
    """Base abstract class for database operations across different database types."""

    def __init__(
        self,
        server: str,
        database: str,
        username: str,
        password: str,
        port: Optional[int] = None
    ) -> None:
        """
        Initialize the Database helper.

        :param server: Database server hostname/address.
        :param database: Database name.
        :param username: Database username.
        :param password: Database password.
        :param port: Database port (optional).
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        logger.info(f"Configured base helper for database: {database} on {server}")

    @abstractmethod
    def connect(self):
        """
        Establish a connection to the database.
        
        :return: Database connection object.
        """
        pass

    @abstractmethod
    def execute_query(self, query: str, params: Optional[Any] = None) -> List[Tuple]:
        """
        Execute a query and return all results.
        
        :param query: SQL query to execute.
        :param params: Parameters for the query (optional).
        :return: List of result rows as tuples.
        """
        pass

    @abstractmethod
    def execute_query_as_dict(self, query: str, params: Optional[Any] = None) -> List[Dict[str, Any]]:
        """
        Execute a query and return results as dictionaries.
        
        :param query: SQL query to execute.
        :param params: Parameters for the query (optional).
        :return: List of result rows as dictionaries.
        """
        pass

    @abstractmethod
    def execute_non_query(self, query: str, params: Optional[Any] = None) -> int:
        """
        Execute a non-query statement (INSERT, UPDATE, DELETE).
        
        :param query: SQL statement to execute.
        :param params: Parameters for the statement (optional).
        :return: Number of affected rows.
        """
        pass

    def execute_query_as_dataframe(self, query: str, params: Optional[Any] = None) -> pd.DataFrame:
        """
        Execute a query and return results as a pandas DataFrame.
        
        :param query: SQL query to execute.
        :param params: Parameters for the query (optional).
        :return: Results as a pandas DataFrame.
        """
        results = self.execute_query_as_dict(query, params)
        return pd.DataFrame(results)

    def get_connection_details(self) -> Dict[str, Any]:
        """Get connection details for logging and debugging purposes."""
        return {
            "server": self.server,
            "database": self.database,
            "username": self.username,
            "port": self.port
        }