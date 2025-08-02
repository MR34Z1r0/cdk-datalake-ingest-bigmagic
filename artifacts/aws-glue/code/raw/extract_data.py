# Import required modules
import concurrent.futures as futures
import datetime as dt
import calendar
import logging
import os
import sys
import argparse
import uuid
import boto3
import pytz
import pandas as pd
import re
import io
import gzip
from dateutil.relativedelta import relativedelta

IS_AWS_GLUE = True
IS_AWS_S3 = True
PARQUET_AVAILABLE = True

import pyarrow as pa
import pyarrow.parquet as pq
 
# Import aje_libs 
try:
    import aje_libs
except ImportError as e:
    # Search for aje_libs.zip file and extract it
    import glob
    import zipfile
    
    search_paths = ['/tmp', '/tmp/glue-python-libs-*', '/glue/lib']
    for search_pattern in search_paths:
        for base_path in glob.glob(search_pattern):
            if os.path.isdir(base_path):
                # Look for aje_libs.zip file
                zip_file = os.path.join(base_path, 'aje_libs.zip')
                if os.path.exists(zip_file):
                    # Extract the zip file
                    extract_path = os.path.join(base_path, 'extracted')
                    try:
                        os.makedirs(extract_path, exist_ok=True)
                        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                            zip_ref.extractall(extract_path)
                        
                        # Add extract path to sys.path if aje_libs is there
                        if 'aje_libs' in os.listdir(extract_path):
                            sys.path.insert(0, extract_path)
                            break
                            
                    except Exception as extract_err:
                        pass
    
    # Try importing again
    try:
        import aje_libs
    except ImportError as e2:
        pass

# Import custom helpers
from aje_libs.common.logger import custom_logger, set_logger_config
from aje_libs.common.helpers.dynamodb_helper import DynamoDBHelper
from aje_libs.common.helpers.s3_helper import S3Helper
from aje_libs.common.helpers.secrets_helper import SecretsHelper
from aje_libs.bd.helpers.datafactory_helper import DatabaseFactoryHelper

# Setup timezone and date variables
TZ_LIMA = pytz.timezone('America/Lima')
YEARS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%Y')
MONTHS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%m')
DAYS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%d')
NOW_LIMA = dt.datetime.now(pytz.utc).astimezone(TZ_LIMA)

class DataExtractor:
    def __init__(self, config):
        self.config = config
        self.logger = custom_logger(__name__)
        
        # Initialize DynamoDB helpers
        self.dynamo_logs_table = self.config['DYNAMO_LOGS_TABLE'].strip()
        self.logs_table_db = DynamoDBHelper(self.dynamo_logs_table, "PROCESS_ID", None)
        self.team = self.config.get('TEAM')
        self.data_source = self.config.get('DATA_SOURCE')
        self.environment = self.config.get('ENVIRONMENT')
        self.project_name = self.config.get('PROJECT_NAME')
        self.region = self.config.get('REGION')
        self.table_name = self.config.get('TABLE_NAME')
        
        # Initialize S3 helper
        self.s3_helper = S3Helper(self.config['S3_RAW_PREFIX'].split("/")[2])
        
        # Load configuration from CSV files in S3
        self._load_csv_configurations()
        
        # Initialize table data and endpoint data
        self._initialize_data()
        
    def _load_csv_configurations(self):
        """Load configuration from CSV files in S3"""
        try:
            import csv
            from io import StringIO
            def load_csv_from_s3(s3_path):
                """Load CSV file from S3 and return as list of dictionaries"""
                s3_client = boto3.client('s3')
                bucket = s3_path.split('/')[2]
                key = '/'.join(s3_path.split('/')[3:])
                
                response = s3_client.get_object(Bucket=bucket, Key=key)
                content = response['Body'].read().decode('latin-1')
                
                csv_data = []
                reader = csv.DictReader(StringIO(content), delimiter=';')
                for row in reader:
                    csv_data.append(row)
                
                return csv_data
            
            def load_csv_from_local(file_path):
                import csv
                """Carga un archivo CSV local y lo devuelve como lista de diccionarios"""
                csv_data = []
                with open(file_path, mode='r', encoding='latin-1') as file:
                    reader = csv.DictReader(file, delimiter=';')
                    for row in reader:
                        csv_data.append(row)
                return csv_data
            
            # Load CSV data
            self.logger.info(f"TABLES_CSV_S3: {self.config['TABLES_CSV_S3']}")
            tables_data = load_csv_from_s3(self.config['TABLES_CSV_S3']) if IS_AWS_S3 else load_csv_from_local(self.config['TABLES_CSV_S3'])
            self.logger.info(f"tables_data: {tables_data[0]}")
            self.logger.info(f"CREDENTIALS_CSV_S3: {self.config['CREDENTIALS_CSV_S3']}")
            credentials_data = load_csv_from_s3(self.config['CREDENTIALS_CSV_S3']) if IS_AWS_S3 else load_csv_from_local(self.config['CREDENTIALS_CSV_S3'])
            self.logger.info(f"credentials_data: {credentials_data[0]}")
            self.logger.info(f"COLUMNS_CSV_S3: {self.config['COLUMNS_CSV_S3']}")
            columns_data = load_csv_from_s3(self.config['COLUMNS_CSV_S3']) if IS_AWS_S3 else load_csv_from_local(self.config['COLUMNS_CSV_S3'])
            self.logger.info(f"columns_data: {columns_data[0]}")

            self.logger.info(f"config: {self.config}")

            # Filter data for current table and database
            table_name = self.config.get('TABLE_NAME')
            endpoint_name = self.config.get('ENDPOINT_NAME')
            environment = self.config.get('ENVIRONMENT')
            
            # Find table configuration
            self.table_data = None
            for row in tables_data:
                if row.get('STAGE_TABLE_NAME', '').upper() == table_name.upper():
                    self.table_data = row
                    self.logger.info(f"Found table configuration for {table_name}")
                    break
            
            if not self.table_data:
                raise Exception(f"Table configuration not found for {table_name}")
            
            # Process the COLUMNS field to handle potential SQL Server identifier length issues
            self._process_columns_field()
            
            # Find database credentials
            self.endpoint_data = None
            for row in credentials_data:
                if (row.get('ENDPOINT_NAME', '') == endpoint_name and 
                    row.get('ENV', '').upper() == environment.upper()):
                    self.endpoint_data = row
                    break
            
            if not self.endpoint_data:
                raise Exception(f"Endpoint credentials not found for {endpoint_name} in {environment}")
            
            # Add ENDPOINT_NAME for compatibility
            self.endpoint_data['ENDPOINT_NAME'] = self.endpoint_data.get('ENDPOINT_NAME', '')
            
            # Apply old logic to determine LOAD_TYPE if not explicitly set
            if not self.table_data.get('LOAD_TYPE') or self.table_data.get('LOAD_TYPE', '').strip() == '':
                if self.table_data.get('SOURCE_TABLE_TYPE', '') == 't':
                    if self.endpoint_data.get('ENDPOINT_NAME', '') == 'SALESFORCE_ING':
                        self.table_data['LOAD_TYPE'] = 'days_off'
                        self.table_data['NUM_DAYS'] = '10'
                    else:
                        self.table_data['LOAD_TYPE'] = 'incremental'
                else:
                    self.table_data['LOAD_TYPE'] = 'full'
            
            self.logger.info(f"Determined LOAD_TYPE: {self.table_data.get('LOAD_TYPE', 'not set')} for table {table_name}")
            
            # Filter columns for current table
            self.columns_metadata = []
            for row in columns_data:
                if row.get('TABLE_NAME', '').upper() == table_name.upper():
                    self.columns_metadata.append(row)
            
            self.logger.info(f"Loaded configuration for table: {table_name}")
            self.logger.info(f"Columns count: {len(self.columns_metadata)}")
            
        except Exception as e:
            self.logger.error(f"Error loading CSV configurations: {str(e)}")
            self._log_error(str(e))
            raise Exception(f"Failed to load CSV configurations: {str(e)}")
        
    def _load_json_configurations(self):
        """Load configuration from JSON parameters passed by CDK (DEPRECATED)"""
        try:
            import json
            
            # Parse JSON configurations from job parameters
            self.table_data = json.loads(self.config['TABLE_CONFIG'])
            self.endpoint_data = json.loads(self.config['DB_CONFIG'])
            self.columns_metadata = json.loads(self.config['COLUMNS_CONFIG'])
            
            self.logger.info(f"Loaded configuration for table: {self.table_name}")
            self.logger.info(f"Columns count: {len(self.columns_metadata)}")
            
        except Exception as e:
            self.logger.error(f"Error loading JSON configurations: {str(e)}")
            self._log_error(str(e))
            raise Exception(f"Failed to load JSON configurations: {str(e)}")
        
    def _initialize_data(self):
        """Initialize table and endpoint data from loaded CSV configurations"""
        try:
            # Set S3 path using loaded data
            team = self.endpoint_data['TEAM']
            data_source = self.endpoint_data['DATA_SOURCE']
            endpoint_name = self.endpoint_data['ENDPOINT_NAME']
            # Get clean table name (remove alias after space) for S3 path
            clean_table_name = self._get_clean_table_name()
            self.day_route = f"{team}/{data_source}/{endpoint_name}/{clean_table_name}/year={YEARS_LIMA}/month={MONTHS_LIMA}/day={DAYS_LIMA}/"
            
            self.s3_raw_path = self.config['S3_RAW_PREFIX'] + self.day_route
            self.bucket = self.config['S3_RAW_PREFIX'].split("/")[2]
            
            # Initialize database connection
            self.init_db_connection()
            
        except Exception as e:
            self.logger.error("Error while searching for table data")
            self.logger.error(e)
            self._log_error(str(e))
            raise Exception(f"Failed to initialize data: {str(e)}")
    
    def init_db_connection(self):
        """Initialize database connection using DatabaseFactoryHelper"""
        try:
            # Get credentials using SecretsHelper
            self.secrets_helper = SecretsHelper(f"{self.config['ENVIRONMENT']}/{self.config['PROJECT_NAME']}/{self.team}/{self.data_source}".lower())
            password = self.secrets_helper.get_secret_value(self.endpoint_data["SRC_DB_SECRET"])
            
            # Record connection information for logging
            self.db_type = self.endpoint_data['BD_TYPE']
            self.server = self.endpoint_data['SRC_SERVER_NAME']
            self.port = self.endpoint_data['DB_PORT_NUMBER']
            self.db_name = self.endpoint_data['SRC_DB_NAME']
            self.username = self.endpoint_data['SRC_DB_USERNAME']
            
            # Additional params based on database type
            additional_params = {}
            
            if self.db_type == 'oracle':
                additional_params['service_name'] = self.db_name
                self.url = f"{self.server}:{self.port}/{self.db_name}"
                self.driver = "oracle.jdbc.driver.OracleDriver"
            elif self.db_type == 'mssql':
                self.url = f"{self.server}:{self.port}"
                self.driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            elif self.db_type == 'mysql':
                additional_params['charset'] = 'utf8mb4'
                self.url = f"{self.server}:{self.port}"
                self.driver = "com.mysql.cj.jdbc.Driver"
            
            # Create database helper using factory
            self.db_helper = DatabaseFactoryHelper.create_helper(
                db_type=self.db_type,
                server=self.server,
                database=self.db_name,
                username=self.username,
                password=password,
                port=int(self.port) if self.port else None,
                **additional_params
            )
            self.logger.info(f"Database connection initialized for {self.db_type} database")
            self.logger.info(f"driver: {self.driver}")
            self.logger.info(f"url: {self.url}")
            
        except Exception as e:
            self.logger.error(f"Error initializing database connection: {str(e)}")
            raise
    
    def _get_clean_table_name(self):
        """
        Extract clean table name from SOURCE_TABLE, removing alias after space.
        For Ingest Bigmagic, the structure is: [sourcetablename] [alias] (alias is optional)
        
        Examples:
        - "mcompa1f m" -> "mcompa1f"
        - "users u" -> "users"
        - "simple_table" -> "simple_table"
        """
        source_table = self.table_data.get('SOURCE_TABLE', self.table_name)
        # Split by space and take only the first part (table name)
        clean_name = source_table.split()[0] if source_table and ' ' in source_table else source_table
        self.logger.info(f"Clean table name extracted: '{source_table}' -> '{clean_name}'")
        return clean_name
    
    def _log_error(self, error_message):
        """Log error to DynamoDB and update table status"""
        try:
            # Send error message via SNS if topic ARN is available
            if 'TOPIC_ARN' in self.config and self.config['TOPIC_ARN']:
                self.send_error_message(self.config['TOPIC_ARN'], self.table_data.get('TARGET_TABLE_NAME', self.table_name), error_message)
            
            # Add log entry to DynamoDB logs table
            clean_table_name = self._get_clean_table_name()
            process_id = f"DLB_{self.table_name.split('_')[0]}_{clean_table_name}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}"
            log = {
                'PROCESS_ID': process_id,
                'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
                'PROJECT_NAME': self.config['PROJECT_NAME'],
                'FLOW_NAME': 'extract_bigmagic',
                'TASK_NAME': 'extract_table_bigmagic',
                'TASK_STATUS': 'error',
                'MESSAGE': error_message,
                'PROCESS_TYPE': 'D' if self.table_data.get('LOAD_TYPE', 'full').strip() in ['incremental'] else 'F',
                'CONTEXT': f"{{server='[{self.endpoint_data['ENDPOINT_NAME']},{self.endpoint_data['SRC_SERVER_NAME']}]', user='{self.endpoint_data['SRC_DB_USERNAME']}', table='{self.table_data.get('SOURCE_TABLE', self.table_name)}'}}"
            }
            self.logs_table_db.put_item(log)
        except Exception as e:
            self.logger.error(f"Failed to log error: {str(e)}")
    
    def _log_success(self):
        """Log success to DynamoDB and update table status"""
        try:
            # Add log entry to DynamoDB logs table
            clean_table_name = self._get_clean_table_name()
            process_id = f"DLB_{self.table_name.split('_')[0]}_{clean_table_name}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}"
            self.logger.info(f"process_id: {process_id}")
            log = {
                'PROCESS_ID': process_id,
                'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
                'PROJECT_NAME': self.config['PROJECT_NAME'],
                'FLOW_NAME': 'extract_bigmagic',
                'TASK_NAME': 'extract_table_bigmagic',
                'TASK_STATUS': 'satisfactorio',
                'MESSAGE': '',
                'PROCESS_TYPE': 'D' if self.table_data.get('LOAD_TYPE', 'full').strip() in ['incremental'] else 'F',
                'CONTEXT': f"{{server='[{self.endpoint_data['ENDPOINT_NAME']},{self.endpoint_data['SRC_SERVER_NAME']}]', user='{self.endpoint_data['SRC_DB_USERNAME']}', table='{self.table_data.get('SOURCE_TABLE', self.table_name)}'}}"
            }
            self.logs_table_db.put_item(log)
            self.logger.info("DynamoDB updated with success status")
        except Exception as e:
            self.logger.error(f"Failed to log success: {str(e)}")
    
    def send_error_message(self, topic_arn, table_name, error):
        """Send error message via SNS"""
        client = boto3.client("sns")
        response = client.publish(
            TopicArn=topic_arn,
            Message=f"Failed table: {table_name} \nStep: raw job \nLog ERROR : {error}"
        )
        return response

    def delete_from_target(self, bucket, s3_raw_path):
        """Delete objects from S3 bucket with specified prefix"""
        try:
            # Use S3Helper to manage deletions
            objects_to_delete = self.s3_helper.list_objects(prefix=s3_raw_path)
            if objects_to_delete:
                keys_to_delete = [obj.get('Key') for obj in objects_to_delete]
                self.s3_helper.delete_objects(keys_to_delete)
                self.logger.info(f"Deleted {len(keys_to_delete)} objects from {s3_raw_path}")
            else:
                self.logger.info(f"No objects found to delete in {s3_raw_path}")
        except Exception as e:
            self.logger.error(f"Error deleting objects from S3: {str(e)}")
            raise e

    def transform_to_dt(self, date):
        """Convert string date to datetime object"""
        start_dt = dt.datetime(
            year=int(date[:4]),
            month=int(date[5:7]),
            day=int(date[8:10]),
            hour=int(date[11:13]),
            minute=int(date[14:16]),
            second=int(date[17:19])
        )
        return start_dt

    def get_limits_for_filter(self, month_diff, data_type):
        """Get lower and upper limits for date filters based on data type"""
        data_type = data_type.strip()
        upper_limit = dt.datetime.now(TZ_LIMA)
        lower_limit = upper_limit - relativedelta(months=(-1*int(month_diff)))
        
        if data_type == "aje_period":
            return lower_limit.strftime('%Y%m'), upper_limit.strftime('%Y%m')
        
        elif data_type == "aje_date":
            _, last_day = calendar.monthrange(upper_limit.year, upper_limit.month)
            upper_limit = upper_limit.replace(day=last_day, tzinfo=TZ_LIMA)
            lower_limit = lower_limit.replace(day=1, tzinfo=TZ_LIMA)
            upper_limit = (upper_limit - dt.datetime(1900, 1, 1, tzinfo=TZ_LIMA)).days + 693596
            lower_limit = (lower_limit - dt.datetime(1900, 1, 1, tzinfo=TZ_LIMA)).days + 693596
            return str(lower_limit), str(upper_limit)
            
        elif data_type == "aje_processperiod":
            _, last_day = calendar.monthrange(upper_limit.year, upper_limit.month)
            upper_limit = upper_limit.replace(day=last_day, tzinfo=TZ_LIMA)
            lower_limit = lower_limit.replace(day=1, tzinfo=TZ_LIMA)
            upper_limit = (upper_limit - dt.datetime(1900, 1, 1, tzinfo=TZ_LIMA)).days + 693596
            lower_limit = (lower_limit - dt.datetime(1900, 1, 1, tzinfo=TZ_LIMA)).days + 693596
            return str(lower_limit), str(upper_limit)
      
        return lower_limit.strftime('%Y%m'), upper_limit.strftime('%Y%m')

    def execute_db_query(self, query):
        """Execute query on the database and return results as DataFrame"""
        try:
            self.logger.info("Executing query on database: {query}")
            # Use the database helper created by the factory
            if hasattr(self.db_helper, 'execute_query_as_dataframe'):
                # Directly use dataframe if supported
                return self.db_helper.execute_query_as_dataframe(query)
            else:
                # Otherwise, convert dict results to DataFrame
                result = self.db_helper.execute_query_as_dict(query)
                return pd.DataFrame(result)
        except Exception as e:
            self.logger.error(f"Error executing database query: {str(e)}")
            raise

    def extract_columns_from_query(self, query):
        """Extract column names from SELECT statement - handles complex expressions, functions, and aliases"""
        try:
            # Remove extra whitespaces but preserve original case for column extraction
            clean_query = ' '.join(query.strip().split())
            
            # Find SELECT and FROM positions (case insensitive)
            select_match = re.search(r'\bSELECT\s+', clean_query, re.IGNORECASE)
            from_match = re.search(r'\bFROM\s+', clean_query, re.IGNORECASE)
            
            if not select_match or not from_match:
                raise ValueError("Could not parse SELECT statement")
            
            # Extract the column part (preserve original case)
            select_start = select_match.end()
            from_start = from_match.start()
            columns_part = clean_query[select_start:from_start].strip()
            
            # Smart split by comma (respecting parentheses, quotes, and string concatenation)
            column_expressions = self._smart_split_columns(columns_part)
            
            columns = []
            for expr in column_expressions:
                expr = expr.strip()
                column_name = self._extract_column_alias_or_name(expr)
                if column_name:
                    columns.append(column_name)
            
            return columns
            
        except Exception as e:
            self.logger.warning(f"Could not extract columns from query: {e}")
            return None

    def _smart_split_columns(self, columns_part):
        """Split columns by comma, respecting parentheses, quotes, and operators"""
        expressions = []
        current_expr = ""
        paren_count = 0
        in_single_quote = False
        in_double_quote = False
        i = 0
        
        while i < len(columns_part):
            char = columns_part[i]
            
            # Handle quotes
            if char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
            elif char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote
            
            # Handle parentheses (only when not in quotes)
            elif char == '(' and not in_single_quote and not in_double_quote:
                paren_count += 1
            elif char == ')' and not in_single_quote and not in_double_quote:
                paren_count -= 1
            
            # Handle comma separation (only when not in quotes and parentheses are balanced)
            elif (char == ',' and paren_count == 0 and 
                not in_single_quote and not in_double_quote):
                if current_expr.strip():
                    expressions.append(current_expr.strip())
                current_expr = ""
                i += 1
                continue
            
            current_expr += char
            i += 1
        
        # Add the last expression
        if current_expr.strip():
            expressions.append(current_expr.strip())
        
        return expressions

    def _extract_column_alias_or_name(self, expression):
        """Extract column alias or name from a column expression"""
        try:
            expr = expression.strip()
            
            # Method 1: Check for explicit AS alias (case insensitive)
            as_match = re.search(r'\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*$', expr, re.IGNORECASE)
            if as_match:
                return as_match.group(1).strip()
            
            # Method 2: Check for implicit alias (space-separated, not within functions)
            # Only if the expression contains functions, operators, or complex expressions
            if any(indicator in expr.lower() for indicator in ['(', '+', '-', '*', '/', 'ltrim', 'rtrim', 'convert', 'cast', 'dbo.', 'case']):
                # Look for implicit alias at the end
                # Split by spaces and get the last part that looks like a column name
                words = expr.split()
                if len(words) >= 2:
                    potential_alias = words[-1].strip()
                    # Check if it's a valid identifier (not a keyword or operator)
                    if (re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', potential_alias) and 
                        potential_alias.lower() not in ['and', 'or', 'not', 'in', 'like', 'is', 'null', 'from', 'where', 'select']):
                        return potential_alias
            
            # Method 3: For simple column names (no functions or operators)
            if not any(indicator in expr.lower() for indicator in ['(', '+', '-', '*', '/', 'ltrim', 'rtrim', 'convert', 'cast', 'dbo.', 'case', "'", '"']):
                # It's a simple column name, clean it up
                clean_name = expr.strip('[]"`').strip()
                if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', clean_name):
                    return clean_name
            
            # Method 4: Try to extract the most relevant column name from complex expressions
            # Look for column names in the expression (exclude function names)
            column_pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'
            potential_columns = re.findall(column_pattern, expr)
            
            if potential_columns:
                # Filter out known function names and keywords
                excluded_words = {
                    'ltrim', 'rtrim', 'convert', 'cast', 'case', 'when', 'then', 'else', 'end',
                    'dbo', 'func_cas_todatetime', 'select', 'from', 'where', 'and', 'or', 'in',
                    'like', 'is', 'null', 'not', 'between', 'exists', 'as', 'varchar', 'int',
                    'datetime', 'char', 'nvarchar', 'decimal', 'float', 'bit'
                }
                
                filtered_columns = [col for col in potential_columns 
                                if col.lower() not in excluded_words]
                
                if filtered_columns:
                    # Return the first meaningful column name found
                    return filtered_columns[0]
            
            # Method 5: Last resort - generate a name based on the expression
            if 'ltrim' in expr.lower() and 'rtrim' in expr.lower() and '+' in expr:
                return 'concatenated_field'
            elif 'func_cas_todatetime' in expr.lower():
                return 'datetime_field'
            elif 'convert' in expr.lower() or 'cast' in expr.lower():
                return 'converted_field'
            else:
                return f'expr_field_{hash(expr) % 1000}'
                
        except Exception as e:
            self.logger.warning(f"Could not extract column name from expression '{expression}': {e}")
            return f'unknown_col_{hash(expression) % 1000}'

    def extract_columns_from_query_specific(self, query):
        """Extract columns specifically designed for your query pattern"""
        try:
            # Para tu query específico, puedes usar regex más específicos
            specific_patterns = [
                # Para: ltrim(rtrim(compania)) + '|' + ltrim(rtrim(transport)) as id
                (r"ltrim\(rtrim\(compania\)\)\s*\+.*?ltrim\(rtrim\(transport\)\)\s+as\s+(\w+)", 1),
                # Para: dbo.func_cas_todatetime(fecultimod,horultimod) lastmodifydate
                (r"dbo\.func_cas_todatetime\([^)]+\)\s+(\w+)", 1),
                # Para columnas simples con o sin alias
                (r"\b(\w+)\s*(?:,|$|\s+from)", 1),
            ]
            
            columns = []
            remaining_query = query
            
            for pattern, group_idx in specific_patterns:
                matches = re.finditer(pattern, remaining_query, re.IGNORECASE)
                for match in matches:
                    column_name = match.group(group_idx)
                    if column_name and column_name not in columns:
                        columns.append(column_name)
                    # Remove the matched part to avoid double-counting
                    remaining_query = remaining_query.replace(match.group(0), '', 1)
            
            # Si no se encontraron columnas con patrones específicos, usar el método genérico
            if not columns:
                return self.extract_columns_from_query(query)
            
            return columns
            
        except Exception as e:
            self.logger.warning(f"Specific column extraction failed: {e}")
            return self.extract_columns_from_query(query)

    def write_dataframe_to_s3_parquet(self, df, s3_path, filename=None):
        """Write pandas DataFrame to S3 in Parquet format"""
        try:
            s3_client = boto3.client('s3')
            
            # Parse S3 path
            if s3_path.startswith('s3://'):
                s3_path = s3_path[5:]  # Remove s3:// prefix
            
            path_parts = s3_path.split('/', 1)
            bucket_name = path_parts[0]
            key_prefix = path_parts[1] if len(path_parts) > 1 else ''
            
            # Generate filename if not provided
            if not filename:
                filename = f"data_{uuid.uuid4().hex[:8]}.parquet"
            
            # Ensure key_prefix ends with /
            if key_prefix and not key_prefix.endswith('/'):
                key_prefix += '/'
            
            parquet_key = key_prefix + filename
            
            # Write as Parquet format
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_bytes = parquet_buffer.getvalue()
            
            if not parquet_key.endswith('.parquet'):
                parquet_key += '.parquet'
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=parquet_key,
                Body=parquet_bytes,
                ContentType='application/octet-stream'
            )
            
            self.logger.info(f"Successfully wrote DataFrame to s3://{bucket_name}/{parquet_key} as Parquet")
            return f"s3://{bucket_name}/{parquet_key}"
            
        except Exception as e:
            self.logger.error(f"Error writing DataFrame to S3 as Parquet: {str(e)}")

    def _process_columns_field(self):
        """Process the COLUMNS field to handle potential SQL Server identifier length issues"""
        try:
            if not self.table_data or 'COLUMNS' not in self.table_data:
                self.logger.info("No COLUMNS field found in table_data")
                return
                
            columns_str = self.table_data['COLUMNS']
            if not columns_str or columns_str.strip() == '':
                self.logger.info("COLUMNS field is empty or None")
                return
                
            self.logger.info(f"Processing COLUMNS field for table: {self.table_data.get('STAGE_TABLE_NAME', 'UNKNOWN')}")
                
            # Split columns by comma, but be careful with complex expressions containing commas
            columns = []
            current_column = ""
            paren_count = 0
            quote_count = 0
            
            for char in columns_str:
                current_column += char
                if char == '(' and quote_count % 2 == 0:
                    paren_count += 1
                elif char == ')' and quote_count % 2 == 0:
                    paren_count -= 1
                elif char == "'":
                    quote_count += 1
                elif char == ',' and paren_count == 0 and quote_count % 2 == 0:
                    columns.append(current_column[:-1].strip())  # Remove the comma
                    current_column = ""
            
            # Add the last column
            if current_column.strip():
                columns.append(current_column.strip())
            
            # Process each individual column - keep all as-is (no truncation)
            processed_columns = []
            
            for i, col in enumerate(columns):
                col = col.strip()
                if not col:
                    continue
                processed_columns.append(col)
            
            # Update the table data with processed columns
            result = ', '.join(processed_columns)
            self.table_data['COLUMNS'] = result
            
            self.logger.info(f"Processed {len(columns)} columns successfully")
                
        except Exception as e:
            self.logger.error(f"ERROR in _process_columns_field: {e}")
            self.logger.error(f"Table: {self.table_data.get('STAGE_TABLE_NAME', 'UNKNOWN') if self.table_data else 'NO_TABLE_DATA'}")
            # Don't raise exception, just log the error and continue with original data

    def _apply_column_processing_to_query(self, columns_str):
        """Apply column processing to any columns string to handle SQL Server identifier length limits"""
        try:
            if not columns_str or columns_str.strip() == '':
                return columns_str
                
            # CRITICAL FIX: Remove problematic double quotes that cause SQL Server parsing issues
            clean_columns = columns_str.strip()
            
            # Check for double quotes and remove them
            double_quote_count = clean_columns.count('"')
            if double_quote_count > 0:
                # CRITICAL: Check if the entire field is wrapped in quotes
                if clean_columns.startswith('"') and clean_columns.endswith('"') and double_quote_count == 2:
                    # Remove the wrapping quotes
                    clean_columns = clean_columns[1:-1]
                else:
                    # REMOVE ALL DOUBLE QUOTES - they cause SQL Server to treat entire sections as single identifiers
                    clean_columns = clean_columns.replace('"', '')
            
            # Additional validation after quote removal
            if clean_columns.strip() == '':
                return columns_str  # Return original to avoid total failure
            
            # Step 2: Split columns more intelligently
            # Use a simpler approach - split by comma but handle nested functions
            columns = []
            current_column = ""
            paren_count = 0
            quote_count = 0
            in_single_quote = False
            
            i = 0
            while i < len(clean_columns):
                char = clean_columns[i]
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
            
            # Step 3: Keep all columns as-is (no truncation)
            processed_columns = []
            
            for idx, col in enumerate(columns):
                col = col.strip()
                if not col:
                    continue
                
                # Keep all columns as-is without any truncation
                processed_columns.append(col)
            
            # Return processed columns
            result = ', '.join(processed_columns)
            return result
                
        except Exception as e:
            return columns_str  # Return original if processing fails

    def get_data(self, query, s3_raw_path, actual_thread, number_threads):
        """Get data from database and write to S3 as Parquet"""
        try:
            self.logger.info(query)
            
            # Execute query
            df = self.execute_db_query(query)
            
            # Drop duplicates
            df = df.drop_duplicates()
            
            # Write to S3 using Parquet format
            if len(df) == 0:
                # Try to get columns from the executed query first
                if len(df.columns) > 0 and not all(col.startswith('Unnamed') for col in df.columns):
                    columns = list(df.columns)
                else:
                    # Extract columns from the query itself
                    columns = self.extract_columns_from_query(query)
                    if not columns:
                        # Fallback to specific extraction method
                        columns = self.extract_columns_from_query_specific(query)
                    
                    if not columns:
                        # Last resort: try structure query
                        try:
                            structure_query = f"SELECT * FROM ({query}) AS subquery WHERE 1=0"
                            structure_df = self.execute_db_query(structure_query)
                            columns = list(structure_df.columns)
                        except:
                            columns = ['unknown_column']
                
                # Create empty DataFrame with extracted columns
                empty_df = pd.DataFrame(columns=columns)
                self.write_dataframe_to_s3_parquet(
                    empty_df, 
                    s3_raw_path, 
                    f"empty_data_{actual_thread}.parquet"
                )
                self.logger.info(f"Written empty Parquet file with headers: {columns}")
            else:
                # For non-empty dataframes, write the data as Parquet
                filename = f"data_thread_{actual_thread}_{uuid.uuid4().hex[:8]}.parquet"
                self.write_dataframe_to_s3_parquet(df, s3_raw_path, filename)
            
            self.logger.info(f"finished thread n: {actual_thread}")
            self.logger.info(f"Data sample: {df.head()}")
            
        except Exception as e:
            self.logger.error(f"Error in get_data: {str(e)}")
            raise
    
    def get_min_max_values(self, partition_column):
        """Get min and max values for a partition column"""
        try:
            source_schema = self.table_data.get('SOURCE_SCHEMA', '')
            source_table = self.table_data.get('SOURCE_TABLE', '')
            
            # Create query to get min and max values
            min_max_query = f"SELECT MIN({partition_column}) as min_val, MAX({partition_column}) as max_val FROM {source_schema}.{source_table} {self.table_data.get('JOIN_EXPR', '')} WHERE {partition_column} <> 0"
            if self.table_data.get('FILTER_EXP', '').strip() != '':
                min_max_query = f"{min_max_query} AND {self.table_data['FILTER_EXP']}"
            self.logger.info(f"Executing min/max query: {min_max_query}")
            
            # Execute query
            df_min_max = self.execute_db_query(min_max_query)
            
            # Get min and max values from the DataFrame and convert to integers
            min_raw = df_min_max['min_val'].iloc[0]
            max_raw = df_min_max['max_val'].iloc[0]

            min_val = int(min_raw) if min_raw is not None else None
            max_val = int(max_raw) if max_raw is not None else None
            
            return min_val, max_val
            
        except Exception as e:
            self.logger.error(f"Error getting min and max values: {str(e)}")
            raise
    
    def get_partitioned_query(self, partition_column, min_val, increment, partition_index, num_partitions):
        """Generate partitioned query based on min/max range using integer values"""
        # Calculate start and end values as integers
        start_value = int(min_val + (increment * partition_index))
        
        # For the last partition, use max_val + 1 to ensure we include the max value
        if partition_index == num_partitions - 1:
            end_value = int(min_val + (increment * (partition_index + 1))) + 1
        else:
            end_value = int(min_val + (increment * (partition_index + 1)))
        
        # Get columns and build the query
        columns_aux = self.table_data['COLUMNS']
        if self.table_data.get('ID_COLUMN', '') != '':
            columns_aux = f"{self.table_data['ID_COLUMN']} as id," + self.table_data['COLUMNS']
        
        # Apply column processing to handle SQL Server identifier length limits
        columns_aux = self._apply_column_processing_to_query(columns_aux)
        
        # Use >= and < for the range to avoid overlaps
        query = f"SELECT {columns_aux} FROM {self.table_data.get('SOURCE_SCHEMA', '')}.{self.table_data.get('SOURCE_TABLE', '')} {self.table_data.get('JOIN_EXPR', '')} WHERE {partition_column} >= {start_value} AND {partition_column} < {end_value}"
        
        # Add additional filters if they exist
        if self.table_data.get('FILTER_EXP', '').strip() != '':
            query += f" AND ({self.table_data['FILTER_EXP']})"
            
        self.logger.info(f"Partitioned query {partition_index}: {query} (Range: {start_value} to {end_value})")
        return query
    
    def get_query_for_date_range(self, start, end):
        """Generate query for a date range"""
        query = self.table_data['QUERY_BY_GLUE']
        
        if 'FILTER_TYPE' in self.table_data.keys():
            start, end = self.change_date_format(start, end, self.table_data['FILTER_TYPE'])
            self.logger.debug(f"Start Date: {start}")
            self.logger.debug(f"End Date: {end}")

        if ',' in self.table_data['FILTER_COLUMN']:
            filter_columns = self.table_data['FILTER_COLUMN'].split(",")
            first_filter = filter_columns[0]
            last_filter = filter_columns[1]

            query += f" WHERE ({first_filter} IS NOT NULL and {first_filter} BETWEEN {start} AND {end}) OR ({last_filter} IS NOT NULL and {last_filter} BETWEEN {start} AND {end})"
        else:
            first_filter = self.table_data['FILTER_COLUMN']
            query += f" WHERE {first_filter} is not null and {first_filter} BETWEEN {start} AND {end}"
            
        self.logger.info(query)
        return query
    
    def change_date_format(self, start, end, date_type):
        """Change date format based on database type"""
        if date_type == 'smalldatetime':
            date_format = f"CONVERT(smalldatetime, 'date_to_replace', 120)"

        elif date_type == 'DATE':
            date_format = f"TO_DATE('date_to_replace', 'YYYY-MM-DD HH24:MI:SS')"
            end = end[:19]
            start = start[:19]

        elif date_type == 'TIMESTAMP(6)':
            date_format = f"TO_TIMESTAMP('date_to_replace', 'YYYY-MM-DD HH24:MI:SS.FF')"

        elif date_type == 'SQL_DATETIME':
            date_format = f"CONVERT(DATETIME, 'date_to_replace',  102)"

        elif date_type == 'BIGINT':
            end = dt.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
            end = int(end.timestamp())
            start = dt.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
            start = int(start.timestamp())
            date_format = "date_to_replace"

        end = date_format.replace("date_to_replace", str(end))
        start = date_format.replace("date_to_replace", str(start))
        return start, end
    
    def create_standard_query(self):
        """Create a standard query for non-incremental loads"""
        columns_aux = self.table_data.get('COLUMNS', '*')
        
        if self.table_data.get('ID_COLUMN', '') != '':
            columns_aux = f"{self.table_data['ID_COLUMN']} as id," + self.table_data.get('COLUMNS', '*')
        
        # Apply column processing to handle SQL Server identifier length limits
        processed_columns = self._apply_column_processing_to_query(columns_aux)
        
        query = f"select {processed_columns} from {self.table_data.get('SOURCE_SCHEMA', 'CAN NOT FIND SCHEMA NAME')}.{self.table_data.get('SOURCE_TABLE', 'CAN NOT FIND TABLE NAME')} {self.table_data.get('JOIN_EXPR', '')} "
        
        print(f"=== COMPLETE QUERY ===")
        print(query)
        print(f"=== END COMPLETE QUERY ===")
        
        if self.table_data.get('FILTER_EXP', '').strip() != '' or self.table_data.get('FILTER_COLUMN', '').strip() != '':
            if self.table_data.get('LOAD_TYPE', 'full') == 'full':
                FILTER_COLUMN = '0=0'
            else:
                lower_limit, upper_limit = self.get_limits_for_filter(
                    self.table_data.get('DELAY_INCREMENTAL_INI', -2), 
                    self.table_data.get('FILTER_DATA_TYPE', ""))
                FILTER_COLUMN = self.table_data.get('FILTER_COLUMN', '1=1').replace('{0}', lower_limit).replace('{1}', upper_limit)
                
            if self.table_data.get('FILTER_EXP', '').strip() != '':
                FILTER_EXP = self.table_data['FILTER_EXP']
            else:
                FILTER_EXP = '0=0'
                
            query += f'where {FILTER_EXP} AND {FILTER_COLUMN}'
            
        print(f"=== FINAL COMPLETE QUERY WITH FILTERS ===")
        print(query)
        print(f"=== END FINAL COMPLETE QUERY ===")
        return query
    
    def determine_load_strategy(self):
        """Determine the load strategy based on table configuration"""
        number_threads = 1
        incremental_load = False

        load_type = self.table_data.get('LOAD_TYPE', '').strip().lower()
        table_type = self.table_data.get('SOURCE_TABLE_TYPE', '')
        partition_column = self.table_data.get('PARTITION_COLUMN', '').strip()

        # Full load with partitioning
        if load_type == 'full' and table_type == 't' and partition_column != '':
            self.logger.info("Full load with partitioning based on min/max range")

            try:
                min_val, max_val = self.get_min_max_values(partition_column)

                if min_val is None or max_val is None:
                    self.logger.warning("MIN o MAX es None, cambiando a carga estándar.")
                    raise ValueError("No min/max")

                range_size = max_val - min_val
                number_threads = 30

                if range_size < number_threads:
                    number_threads = max(1, range_size)
                    self.logger.info(f"Reduciendo número de particiones a {number_threads} (rango: {range_size})")

                increment = max(1, range_size // number_threads)

                self.logger.info(
                    f"Partition column: {partition_column}, Min: {min_val}, Max: {max_val}, "
                    f"Range: {range_size}, Increment: {increment}, Partitions: {number_threads}"
                )

                return {
                    'load_type': 'partitioned_full',
                    'number_threads': number_threads,
                    'incremental_load': True,
                    'partition_column': partition_column,
                    'min_val': min_val,
                    'max_val': max_val,
                    'increment': increment
                }

            except Exception as e:
                self.logger.warning(f"No se pudo determinar min/max. Usando estrategia estándar. Motivo: {e}")

        # Incremental between-date load
        if load_type == 'between-date':
            self.logger.info("Incremental load with date range")

            start = self.table_data.get('START_VALUE', '').strip()
            end = self.table_data.get('END_VALUE', '').strip()

            if not start or not end:
                self.logger.warning("START_VALUE o END_VALUE no definidos. Usando estrategia estándar.")
                return {
                    'load_type': 'standard',
                    'number_threads': number_threads,
                    'incremental_load': incremental_load
                }

            try:
                number_threads = int(self.config.get('THREADS_FOR_INCREMENTAL_LOADS', 1))
            except ValueError:
                number_threads = 1
                self.logger.warning("THREADS_FOR_INCREMENTAL_LOADS inválido, usando 1 hilo.")

            start_dt = self.transform_to_dt(start)
            end_dt = self.transform_to_dt(end)
            delta = (end_dt - start_dt) / number_threads

            return {
                'load_type': 'between_date',
                'number_threads': number_threads,
                'incremental_load': True,
                'start_dt': start_dt,
                'end_dt': end_dt,
                'delta': delta
            }

        # Default case
        self.logger.info("Usando carga estándar")
        return {
            'load_type': 'standard',
            'number_threads': number_threads,
            'incremental_load': incremental_load
        }
    
    def extract_data(self):
        """Main method to extract data from source and load to S3 with controlled concurrency"""
        try:
            # Delete existing data in the target S3 path
            self.delete_from_target(self.bucket, self.day_route)
            
            # Determine load strategy
            load_strategy = self.determine_load_strategy()
            total_tasks = load_strategy['number_threads']
            incremental_load = load_strategy['incremental_load']
            
            # Set maximum concurrent workers (6 as requested)
            max_concurrent_workers = min(6, total_tasks)
            self.logger.info(f"Processing {total_tasks} tasks with maximum {max_concurrent_workers} concurrent workers")
            
            # Generate all queries upfront based on load strategy
            all_queries = []
            for i in range(total_tasks):
                if incremental_load:
                    if load_strategy['load_type'] == 'partitioned_full':
                        # Generate partitioned query for full load with partitioning
                        query = self.get_partitioned_query(
                            load_strategy['partition_column'],
                            load_strategy['min_val'],
                            load_strategy['increment'],
                            i,
                            total_tasks
                        )
                    else:
                        # Generate query for date-based incremental load
                        start_dt = load_strategy['start_dt']
                        delta = load_strategy['delta']
                        start_str = str(start_dt + delta * i)[:19]
                        end_str = str(start_dt + delta * (i + 1))[:19]
                        query = self.get_query_for_date_range(start_str, end_str)
                else:
                    # Generate standard query for non-incremental loads
                    query = self.create_standard_query()
                
                # Final query print for debugging
                print(f"Final Query {i}: {query}")
                
                all_queries.append(query)
            
            # Process tasks in batches with controlled concurrency
            completed_tasks = 0
            active_futures = set()
            
            with futures.ThreadPoolExecutor(max_workers=max_concurrent_workers) as executor:
                # Initial batch of tasks
                for i in range(min(max_concurrent_workers, total_tasks)):
                    future = executor.submit(
                        self.get_data,
                        all_queries[i],
                        self.s3_raw_path,
                        i,
                        total_tasks
                    )
                    active_futures.add(future)
                    
                # Process tasks as they complete
                next_task_idx = max_concurrent_workers
                
                while active_futures and completed_tasks < total_tasks:
                    # Wait for any task to complete
                    done, active_futures = futures.wait(
                        active_futures, 
                        return_when=futures.FIRST_COMPLETED
                    )
                    
                    # Process completed tasks
                    for future in done:
                        try:
                            future.result()  # Check for exceptions
                            completed_tasks += 1
                            self.logger.info(f"Completed task {completed_tasks}/{total_tasks}")
                        except Exception as e:
                            self.logger.error(f"Task failed with error: {str(e)}")
                            raise
                    
                    # Queue up new tasks if available
                    while len(active_futures) < max_concurrent_workers and next_task_idx < total_tasks:
                        future = executor.submit(
                            self.get_data,
                            all_queries[next_task_idx],
                            self.s3_raw_path,
                            next_task_idx,
                            total_tasks
                        )
                        active_futures.add(future)
                        self.logger.info(f"Started task {next_task_idx + 1}/{total_tasks}")
                        next_task_idx += 1
            
            self.logger.info(f"All {total_tasks} tasks completed successfully")
            
            # Log success
            self._log_success()
            
            return True
                
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Error in extract_data: {error_msg}")
            self._log_error(error_msg)
            raise Exception(f"Failed to extract data: {error_msg}")

config = {}
if IS_AWS_GLUE:
    from awsglue.utils import getResolvedOptions    
    args = getResolvedOptions(
        sys.argv, ['S3_RAW_PREFIX', 'ARN_TOPIC_SUCCESS', 'PROJECT_NAME', 'TEAM', 'DATA_SOURCE', 'ENVIRONMENT', 'REGION', 'DYNAMO_LOGS_TABLE', 'ARN_TOPIC_FAILED', 'TABLE_NAME', 'TABLES_CSV_S3', 'CREDENTIALS_CSV_S3', 'COLUMNS_CSV_S3', 'ENDPOINT_NAME'])

    config = {
        "S3_RAW_PREFIX": args["S3_RAW_PREFIX"],
        "DYNAMO_LOGS_TABLE": args["DYNAMO_LOGS_TABLE"],
        "ENVIRONMENT": args["ENVIRONMENT"],
        "PROJECT_NAME": args["PROJECT_NAME"],
        "TEAM": args["TEAM"],
        "DATA_SOURCE": args["DATA_SOURCE"],
        "THREADS_FOR_INCREMENTAL_LOADS": 6,
        "TOPIC_ARN": args["ARN_TOPIC_FAILED"], 
        "REGION": args["REGION"],
        "TABLE_NAME": args["TABLE_NAME"],
        "TABLES_CSV_S3": args["TABLES_CSV_S3"],
        "CREDENTIALS_CSV_S3": args["CREDENTIALS_CSV_S3"],
        "COLUMNS_CSV_S3": args["COLUMNS_CSV_S3"],
        "ENDPOINT_NAME": args["ENDPOINT_NAME"]
    }
region_name = config["REGION"]
#boto3.setup_default_session(profile_name='prod-compliance-admin', region_name=region_name)

logger = custom_logger(__name__)

logger.info("=" * 80)

logger.info("Version: SQL Server Identifier Parsing Fix v2.0")
logger.info(f"Table: {config['TABLE_NAME']}")
logger.info(f"EndPoint: {config['ENDPOINT_NAME']}")
logger.info("=" * 80)

logger.info("Starting data extraction process")
logger.info(f"Configuration: {config}")

try:
    # Create extractor instance
    logger.info("Creating extractor instance")
    extractor = DataExtractor(config)
    logger.info("Extractor instance created")
    # Run extraction
    success = extractor.extract_data()
    logger.info("Extraction completed")
    if success:
        logger.info(f"Successfully extracted data for table {config['TABLE_NAME']}") 
    else:
        logger.error(f"Failed to extract data for table {config['TABLE_NAME']}") 
        
except Exception as e:
    logger.error(f"An error occurred during extraction: {str(e)}")
    raise e