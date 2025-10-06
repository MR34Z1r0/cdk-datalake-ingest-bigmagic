# -*- coding: utf-8 -*-
import time
import pandas as pd
from typing import Optional, Tuple, Iterator, Dict, Any
from datetime import datetime
from interfaces.extractor_interface import ExtractorInterface
from models.database_config import DatabaseConfig
from exceptions.custom_exceptions import ConnectionError, ExtractionError
from aje_libs.common.helpers.secrets_helper import SecretsHelper

try:
    import sqlalchemy
    from sqlalchemy import create_engine, text
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False
    import pymssql

class SQLServerExtractor(ExtractorInterface):
    """SQL Server implementation with SQLAlchemy support for better pandas compatibility"""
    
    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
        self.connection = None
        self.engine = None
        self._secrets_helper = None
        self._password = None
        self.max_retries = 3
        self.retry_delay = 5
        self.use_sqlalchemy = True  # Prefer SQLAlchemy when available

        from aje_libs.common.datalake_logger import DataLakeLogger
        self.logger = DataLakeLogger.get_logger(__name__)
    
    def connect(self):
        """Establish connection using SQLAlchemy engine for better pandas compatibility"""
        try:
            if not self._password:
                self._get_password()
            
            if HAS_SQLALCHEMY and self.use_sqlalchemy:
                self._connect_sqlalchemy()
            else:
                self._connect_pymssql()
                
        except Exception as e:
            raise ConnectionError(f"Failed to connect to SQL Server: {e}")
    
    def _connect_sqlalchemy(self):
        """Connect using SQLAlchemy engine"""
        try:
            self.logger.info("=" * 80)
            self.logger.info("ESTABLISHING DATABASE CONNECTION (SQLAlchemy)")
            self.logger.info("=" * 80)
            self.logger.info(f"Server: {self.config.server}")
            self.logger.info(f"Database: {self.config.database}")
            self.logger.info(f"User: {self.config.username}")
            self.logger.info(f"Port: {self.config.port or 1433}")
            
            # Create SQLAlchemy connection string
            connection_string = (
                f"mssql+pymssql://{self.config.username}:{self._password}"
                f"@{self.config.server}:{self.config.port or 1433}/{self.config.database}"
                f"?charset=utf8&timeout=900&login_timeout=900"
            )
            
            # Create engine with connection pooling
            self.engine = create_engine(
                connection_string,
                pool_size=3,
                max_overflow=5,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.logger.info("✅ SQLAlchemy engine connected successfully")
            self.logger.info(f"Connection pool size: 3, max overflow: 5")
            self.logger.info("=" * 80)
            
        except Exception as e:
            self.logger.warning(f"❌ SQLAlchemy connection failed: {e}")
            self.logger.info("🔄 Falling back to pymssql...")
            self.use_sqlalchemy = False
            self._connect_pymssql()
    
    def _connect_pymssql(self):
        """Fallback to pymssql connection"""
        import pymssql
        
        self.logger.info("=" * 80)
        self.logger.info("ESTABLISHING DATABASE CONNECTION (PyMSSQL)")
        self.logger.info("=" * 80)
        self.logger.info(f"Server: {self.config.server}")
        self.logger.info(f"Database: {self.config.database}")
        self.logger.info(f"User: {self.config.username}")
        self.logger.info(f"Port: {self.config.port or 1433}")
        
        self.connection = pymssql.connect(
            server=self.config.server,
            user=self.config.username,
            password=self._password,
            database=self.config.database,
            port=self.config.port or 1433,
            timeout=900,
            login_timeout=900,
            charset='utf8'
        )
        
        self.logger.info("✅ PyMSSQL connection established")
        self.logger.info("=" * 80)
    
    def test_connection(self) -> bool:
        """Test connection to SQL Server"""
        try:
            if not self.connection and not self.engine:
                self.connect()
            
            if self.engine:
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1 as test"))
                self.logger.info("✅ Database connection test successful (SQLAlchemy)")
                return True
            else:
                cursor = self.connection.cursor()
                cursor.execute("SELECT 1 as test")
                result = cursor.fetchone()
                cursor.close()
                self.logger.info("✅ Database connection test successful (PyMSSQL)")
                return result is not None
                
        except Exception as e:
            self.logger.error(f"❌ Connection test failed: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> pd.DataFrame:
        """Execute query with retry logic and better error handling"""
        for attempt in range(self.max_retries):
            try:
                if not self.connection and not self.engine:
                    self.connect()
                
                self.logger.info(f"🔍 Query Attempt {attempt + 1}/{self.max_retries}")
                self.logger.info(f"Query preview: {query}...")
                
                start_time = datetime.now()
                
                if self.engine:
                    # Use SQLAlchemy engine
                    if params:
                        df = pd.read_sql(text(query), self.engine, params=params)
                    else:
                        df = pd.read_sql(text(query), self.engine)
                else:
                    # Use pymssql connection
                    if params:
                        df = pd.read_sql(query, self.connection, params=params)
                    else:
                        df = pd.read_sql(query, self.connection)
                
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                # Fix duplicate column names
                df = self._fix_duplicate_columns(df)
                
                self.logger.info(f"✅ Query executed successfully")
                self.logger.info(f"Execution time: {duration:.2f}s")
                self.logger.info(f"Rows returned: {len(df):,}")
                self.logger.info(f"Columns: {len(df.columns)}")
                
                return df
                
            except Exception as e:
                self.logger.error(f"❌ Attempt {attempt + 1} failed: {str(e)}")
                
                if attempt < self.max_retries - 1:
                    self.logger.info(f"⏳ Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                    
                    # Recreate connection on retry
                    try:
                        self.close()
                        self.connect()
                    except:
                        pass
                else:
                    self.logger.error(f"❌ All {self.max_retries} attempts failed")
                    raise ExtractionError(f"Failed to execute query after {self.max_retries} attempts: {e}")
    
    def execute_query_chunked(self, query: str, chunk_size: int, order_by: str, params: Optional[Tuple] = None) -> Iterator[pd.DataFrame]:
        """Execute query in chunks using OFFSET/FETCH"""
        try:
            self.logger.info("=" * 80)
            self.logger.info("CHUNKED QUERY EXECUTION")
            self.logger.info("=" * 80)
            self.logger.info(f"Chunk size: {chunk_size:,}")
            self.logger.info(f"Order by: {order_by}")
            
            offset = 0
            chunk_number = 0
            
            while True:
                chunk_number += 1
                
                # Build chunked query
                chunked_query = f"""
                {query}
                ORDER BY {order_by}
                OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY
                """
                
                self.logger.info("-" * 80)
                self.logger.info(f"Chunk #{chunk_number}: offset={offset:,}, size={chunk_size:,}")
                self.logger.info("SQL Query:")
                self.logger.info(chunked_query)
                self.logger.info("-" * 80)
                
                # Execute chunk query
                chunk_df = self.execute_query(chunked_query, params)
                
                self.logger.info(f"Chunk #{chunk_number} returned {len(chunk_df):,} rows")
                
                if chunk_df.empty:
                    self.logger.info(f"Empty chunk received, stopping pagination")
                    break
                    
                yield chunk_df
                
                # If we got fewer rows than chunk_size, we've reached the end
                if len(chunk_df) < chunk_size:
                    self.logger.info(f"Last chunk received ({len(chunk_df):,} < {chunk_size:,}), stopping pagination")
                    break
                    
                offset += chunk_size
            
            self.logger.info("=" * 80)
            self.logger.info(f"CHUNKED EXECUTION COMPLETED - Total chunks: {chunk_number}")
            self.logger.info("=" * 80)
                
        except Exception as e:
            self.logger.error(f"❌ Chunked extraction failed: {e}")
            raise ExtractionError(f"Failed chunked extraction: {e}")
    
    def extract_data(self, query: str, chunk_size: Optional[int] = None, 
                 order_by: Optional[str] = None, 
                 params: Optional[Tuple] = None) -> Iterator[pd.DataFrame]:
        """
        Extract data using query - main extraction method
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info("DATA EXTRACTION STARTED")
            self.logger.info("=" * 80)
            
            if chunk_size and order_by:
                self.logger.info("Extraction mode: CHUNKED")
                self.logger.info(f"Chunk size: {chunk_size:,}")
                self.logger.info(f"Order by: {order_by}")
                self.logger.info("-" * 80)
                self.logger.info("SQL Query:")
                self.logger.info(query)
                self.logger.info("=" * 80)
                
                # Use chunked extraction
                chunk_count = 0
                for chunk_df in self.execute_query_chunked(query, chunk_size, order_by, params):
                    chunk_count += 1
                    self.logger.info(f"Yielding chunk {chunk_count} with {len(chunk_df):,} rows")
                    yield chunk_df
                
                self.logger.info(f"Chunked extraction completed - Total chunks yielded: {chunk_count}")
            else:
                self.logger.info("Extraction mode: SINGLE QUERY")
                self.logger.info("-" * 80)
                self.logger.info("SQL Query:")
                self.logger.info(query)
                self.logger.info("=" * 80)
                
                df = self.execute_query(query, params)
                
                if not df.empty:
                    self.logger.info(f"Yielding single result with {len(df):,} rows")
                    yield df
                else:
                    self.logger.warning("Query returned empty result")
            
            self.logger.info("=" * 80)
            self.logger.info("DATA EXTRACTION COMPLETED")
            self.logger.info("=" * 80)
                    
        except Exception as e:
            self.logger.error("=" * 80)
            self.logger.error("❌ DATA EXTRACTION FAILED")
            self.logger.error("=" * 80)
            self.logger.error(f"Error: {str(e)}")
            self.logger.error(f"Error type: {type(e).__name__}")
            
            import traceback
            self.logger.error("Traceback:")
            self.logger.error(traceback.format_exc())
            self.logger.error("=" * 80)
            
            raise ExtractionError(f"Failed to extract data: {e}")
    
    def get_min_max_values(self, query: str) -> Tuple[Optional[int], Optional[int]]:
        """Get min and max values from query"""
        try:
            self.logger.info("Executing MIN/MAX query")
            df = self.execute_query(query)
            
            if df.empty:
                self.logger.warning("MIN/MAX query returned empty result")
                return None, None
            
            min_val = df.iloc[0]['min_val'] if 'min_val' in df.columns else None
            max_val = df.iloc[0]['max_val'] if 'max_val' in df.columns else None
            
            min_val = int(min_val) if min_val is not None else None
            max_val = int(max_val) if max_val is not None else None
            
            self.logger.info(f"MIN/MAX values: min={min_val}, max={max_val}")
            
            return min_val, max_val
            
        except Exception as e:
            self.logger.error(f"Failed to get MIN/MAX values: {e}")
            raise ExtractionError(f"Failed to get min/max values: {e}")
    
    def close(self):
        """Close connections"""
        if self.engine:
            try:
                self.engine.dispose()
                self.logger.info("🔒 SQLAlchemy engine disposed")
            except Exception:
                pass
            finally:
                self.engine = None
                
        if self.connection:
            try:
                self.connection.close()
                self.logger.info("🔒 PyMSSQL connection closed")
            except Exception:
                pass
            finally:
                self.connection = None
    
    def _get_password(self):
        """Get password from secrets manager"""
        if not self._secrets_helper:
            secret_path = f"{self.config.secret_name.lower()}"
            self._secrets_helper = SecretsHelper(secret_path)
        
        self._password = self._secrets_helper.get_secret_value(self.config.secret_key)
    
    def _fix_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fix duplicate column names"""
        if df.empty:
            return df
        
        columns = list(df.columns)
        if len(columns) != len(set(columns)):
            seen = {}
            new_columns = []
            
            for col in columns:
                if col in seen:
                    seen[col] += 1
                    new_col = f"{col}_{seen[col]}"
                else:
                    seen[col] = 0
                    new_col = col
                new_columns.append(new_col)
            
            df.columns = new_columns
        
        return df