# -*- coding: utf-8 -*-
import time
import pandas as pd
from typing import Optional, Tuple, Iterator, Dict, Any
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

        # Agregar esta línea para configurar el logger
        from aje_libs.common.logger import custom_logger
        self.logger = custom_logger(__name__)
    
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
            
            print("✅ SQLAlchemy engine connected successfully")
            
        except Exception as e:
            print(f"❌ SQLAlchemy connection failed: {e}")
            print("🔄 Falling back to pymssql...")
            self.use_sqlalchemy = False
            self._connect_pymssql()
    
    def _connect_pymssql(self):
        """Fallback to pymssql connection"""
        import pymssql
        
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
        print("✅ PyMSSQL connection established")
    
    def test_connection(self) -> bool:
        """Test connection to SQL Server"""
        try:
            if not self.connection and not self.engine:
                self.connect()
            
            if self.engine:
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1 as test"))
                return True
            else:
                cursor = self.connection.cursor()
                cursor.execute("SELECT 1 as test")
                result = cursor.fetchone()
                cursor.close()
                return result is not None
                
        except Exception:
            return False
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> pd.DataFrame:
        """Execute query with retry logic and better error handling"""
        for attempt in range(self.max_retries):
            try:
                if not self.connection and not self.engine:
                    self.connect()
                
                print(f"🔍 QUERY ATTEMPT {attempt + 1}: {query[:100]}...")
                
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
                
                # Fix duplicate column names
                df = self._fix_duplicate_columns(df)
                
                return df
                
            except Exception as e:
                print(f"❌ Attempt {attempt + 1} failed: {str(e)}")
                
                if attempt < self.max_retries - 1:
                    print(f"⏳ Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                    
                    # Recreate connection on retry
                    try:
                        self.close()
                        self.connect()
                    except:
                        pass
                else:
                    raise ExtractionError(f"Failed to execute query after {self.max_retries} attempts: {e}")
    
    def execute_query_chunked(self, query: str, chunk_size: int, order_by: str, params: Optional[Tuple] = None) -> Iterator[pd.DataFrame]:
        """Execute query in chunks using OFFSET/FETCH"""
        try:
            offset = 0
            while True:
                # Build chunked query
                chunked_query = f"""
                {query}
                ORDER BY {order_by}
                OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY
                """
                
                print(f"🔍 Executing chunk: offset={offset}, size={chunk_size}")
                self.logger.info(f"🔍 CHUNKED: Executing chunk offset={offset}, size={chunk_size}")
                self.logger.info(f"🔍 CHUNKED: Full query: {chunked_query}")
                
                # Execute chunk query
                chunk_df = self.execute_query(chunked_query, params)
                
                self.logger.info(f"🔍 CHUNKED: Chunk returned {len(chunk_df) if chunk_df is not None else 'None'} rows")
                
                if chunk_df.empty:
                    self.logger.info(f"🔍 CHUNKED: Empty chunk received, stopping pagination")
                    break
                    
                yield chunk_df
                
                # If we got fewer rows than chunk_size, we've reached the end
                if len(chunk_df) < chunk_size:
                    self.logger.info(f"🔍 CHUNKED: Last chunk received ({len(chunk_df)} < {chunk_size}), stopping pagination")
                    break
                    
                offset += chunk_size
                
        except Exception as e:
            self.logger.error(f"🔍 CHUNKED ERROR: Failed chunked extraction: {e}")
            raise ExtractionError(f"Failed chunked extraction: {e}")
    
    def extract_data(self, query: str, chunk_size: Optional[int] = None, 
                 order_by: Optional[str] = None, 
                 params: Optional[Tuple] = None) -> Iterator[pd.DataFrame]:
        """
        Extract data using query - main extraction method
        """
        try:
            if chunk_size and order_by:
                print("QUERY_CHUNKED")
                print(f"CHUNK SIZE: {chunk_size}  ORDER BY: {order_by} PARAMS: {params}")
                self.logger.info(f"🔍 EXTRACTOR: Starting chunked extraction with chunk_size={chunk_size}, order_by={order_by}")
                # Use chunked extraction
                chunk_count = 0
                for chunk_df in self.execute_query_chunked(query, chunk_size, order_by, params):
                    chunk_count += 1
                    self.logger.info(f"🔍 EXTRACTOR: Generated chunk {chunk_count} with {len(chunk_df) if chunk_df is not None else 'None'} rows")
                    yield chunk_df
                self.logger.info(f"🔍 EXTRACTOR: Completed chunked extraction, total chunks generated: {chunk_count}")
            else:
                # Execute as single query
                print("QUERY_SINGLE")
                print(f"PARAMS: {params}")
                self.logger.info(f"🔍 EXTRACTOR: Starting single query extraction")
                df = self.execute_query(query, params)
                self.logger.info(f"🔍 EXTRACTOR: Single query returned {len(df) if df is not None else 'None'} rows")
                if not df.empty:
                    yield df
                    
        except Exception as e:
            self.logger.error(f"🔍 EXTRACTOR ERROR: Failed to extract data: {e}")
            import traceback
            self.logger.error(f"🔍 EXTRACTOR TRACEBACK: {traceback.format_exc()}")
            raise ExtractionError(f"Failed to extract data: {e}")
    
    def get_min_max_values(self, query: str) -> Tuple[Optional[int], Optional[int]]:
        """Get min and max values from query"""
        try:
            df = self.execute_query(query)
            if df.empty:
                return None, None
            
            min_val = df.iloc[0]['min_val'] if 'min_val' in df.columns else None
            max_val = df.iloc[0]['max_val'] if 'max_val' in df.columns else None
            
            min_val = int(min_val) if min_val is not None else None
            max_val = int(max_val) if max_val is not None else None
            
            return min_val, max_val
            
        except Exception as e:
            raise ExtractionError(f"Failed to get min/max values: {e}")
    
    def close(self):
        """Close connections"""
        if self.engine:
            try:
                self.engine.dispose()
                print("🔒 SQLAlchemy engine disposed")
            except Exception:
                pass
            finally:
                self.engine = None
                
        if self.connection:
            try:
                self.connection.close()
                print("🔒 PyMSSQL connection closed")
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