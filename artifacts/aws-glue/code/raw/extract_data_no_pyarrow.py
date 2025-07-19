# Alternative implementation without PyArrow dependency
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
import json
from dateutil.relativedelta import relativedelta
from awsglue.utils import getResolvedOptions

# Debug: Print Python path and check for aje_libs
print("Python path:")
for path in sys.path:
    print(f"  {path}")

print("Looking for aje_libs...")
try:
    import aje_libs
    print(f"aje_libs found at: {aje_libs.__file__}")
except ImportError as e:
    print(f"aje_libs not found: {e}")
    
    # Search for aje_libs.zip file and extract it
    import glob
    import zipfile
    
    search_paths = ['/tmp', '/tmp/glue-python-libs-*', '/glue/lib']
    for search_pattern in search_paths:
        print(f"Searching in: {search_pattern}")
        for base_path in glob.glob(search_pattern):
            if os.path.isdir(base_path):
                print(f"  Checking directory: {base_path}")
                
                # Look for aje_libs.zip file
                zip_file = os.path.join(base_path, 'aje_libs.zip')
                if os.path.exists(zip_file):
                    print(f"  Found aje_libs.zip at: {zip_file}")
                    
                    # Extract the zip file
                    extract_path = os.path.join(base_path, 'extracted')
                    try:
                        os.makedirs(extract_path, exist_ok=True)
                        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                            zip_ref.extractall(extract_path)
                        print(f"  Extracted to: {extract_path}")
                        
                        # List contents of extracted directory
                        print(f"  Extracted contents:")
                        for item in os.listdir(extract_path):
                            item_path = os.path.join(extract_path, item)
                            if os.path.isdir(item_path):
                                print(f"    DIR:  {item}")
                            else:
                                print(f"    FILE: {item}")
                        
                        # Add extract path to sys.path if aje_libs is there
                        if 'aje_libs' in os.listdir(extract_path):
                            sys.path.insert(0, extract_path)
                            print(f"  Added to sys.path: {extract_path}")
                            break
                            
                    except Exception as extract_err:
                        print(f"  Error extracting zip: {extract_err}")
    
    # Try importing again
    try:
        import aje_libs
        print(f"SUCCESS: aje_libs now found at: {aje_libs.__file__}")
    except ImportError as e2:
        print(f"Still cannot import aje_libs: {e2}")
        print("Final sys.path:")
        for path in sys.path:
            print(f"  {path}")

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
        self.s3_helper = S3Helper()
        
        # Load configuration from JSON parameters
        self._load_json_configurations()
        
        # Initialize table data and endpoint data
        self._initialize_data()
        
    def _load_json_configurations(self):
        """Load configuration from JSON parameters passed by CDK"""
        try:
            # Parse JSON configurations from job parameters
            self.table_data = json.loads(self.config['TABLE_CONFIG'])
            self.endpoint_data = json.loads(self.config['DB_CONFIG'])
            self.columns_metadata = json.loads(self.config['COLUMNS_CONFIG'])
            
            self.logger.info(f"Loaded configuration for table: {self.table_name}")
            self.logger.info(f"Table config: {self.table_data}")
            self.logger.info(f"DB config keys: {list(self.endpoint_data.keys())}")
            self.logger.info(f"Columns count: {len(self.columns_metadata)}")
            
        except Exception as e:
            self.logger.error(f"Error loading JSON configurations: {str(e)}")
            self._log_error(str(e))
            raise Exception(f"Failed to load JSON configurations: {str(e)}")
        
    def _initialize_data(self):
        """Initialize table and endpoint data from loaded CSV configurations"""
        try:
            # Set S3 path using loaded data
            team = self.endpoint_data.get('TEAM', self.team)
            data_source = self.endpoint_data.get('DATA_SOURCE', self.data_source)
            self.day_route = f"{team}/{data_source}/{self.table_name}/{self.table_data['SOURCE_TABLE']}/year={YEARS_LIMA}/month={MONTHS_LIMA}/day={DAYS_LIMA}/"
            
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
            self.secrets_helper = SecretsHelper(f"{self.config['ENVIRONMENT']}/{self.config['PROJECT_NAME']}/{self.team}/{self.data_source}")
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

    def write_dataframe_to_s3_parquet_fallback(self, df, s3_path, filename=None):
        """Write pandas DataFrame to S3 as Parquet using pandas engine (fallback without PyArrow)"""
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
            
            key = key_prefix + filename
            
            # Try to use pandas built-in parquet support first
            try:
                # Convert DataFrame to Parquet bytes using pandas engine
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, engine='fastparquet', compression='snappy', index=False)
                parquet_bytes = parquet_buffer.getvalue()
                
                # Upload to S3
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=parquet_bytes,
                    ContentType='application/octet-stream'
                )
                
                self.logger.info(f"Successfully wrote DataFrame to s3://{bucket_name}/{key} using fastparquet")
                return f"s3://{bucket_name}/{key}"
                
            except Exception as fastparquet_error:
                self.logger.warning(f"FastParquet failed: {fastparquet_error}, trying alternative method")
                
                # Fallback to CSV format with parquet extension for compatibility
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False, sep='|', quoting=1)
                csv_bytes = csv_buffer.getvalue().encode('utf-8')
                
                # Use .csv extension to be clear about format
                csv_key = key.replace('.parquet', '.csv')
                
                # Upload to S3
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=csv_key,
                    Body=csv_bytes,
                    ContentType='text/csv'
                )
                
                self.logger.info(f"Successfully wrote DataFrame to s3://{bucket_name}/{csv_key} as CSV (fallback)")
                return f"s3://{bucket_name}/{csv_key}"
                
        except Exception as e:
            self.logger.error(f"Error writing DataFrame to S3: {str(e)}")
            raise

    def execute_db_query(self, query):
        """Execute query on the database and return results as DataFrame"""
        try:
            self.logger.info(f"Executing query on database: {query}")
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

    def get_data(self, query, s3_raw_path, actual_thread, number_threads):
        """Get data from database and write to S3"""
        try:
            self.logger.info(f"Thread {actual_thread}: {query}")
            
            # Execute query
            df = self.execute_db_query(query)
            
            # Drop duplicates
            df = df.drop_duplicates()
            
            # Write to S3 using fallback method
            if len(df) == 0:
                # Create empty DataFrame with proper columns
                columns = list(df.columns) if len(df.columns) > 0 else ['unknown_column']
                empty_df = pd.DataFrame(columns=columns)
                self.write_dataframe_to_s3_parquet_fallback(
                    empty_df, 
                    s3_raw_path, 
                    f"empty_data_{actual_thread}.parquet"
                )
                self.logger.info(f"Written empty file with headers: {columns}")
            else:
                # For non-empty dataframes, write the data
                filename = f"data_thread_{actual_thread}_{uuid.uuid4().hex[:8]}.parquet"
                self.write_dataframe_to_s3_parquet_fallback(df, s3_raw_path, filename)
            
            self.logger.info(f"Finished thread {actual_thread}")
            self.logger.info(f"Data sample: {df.head()}")
            
        except Exception as e:
            self.logger.error(f"Error in get_data: {str(e)}")
            raise

    def extract_data(self):
        """Main method to extract data from source and load to S3"""
        try:
            # Delete existing data in the target S3 path
            self.delete_from_target(self.bucket, self.day_route)
            
            # For simplicity, use a basic query approach
            query = self.create_standard_query()
            
            # Execute extraction
            self.get_data(query, self.s3_raw_path, 0, 1)
            
            # Log success
            self._log_success()
            
            return True
                
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Error in extract_data: {error_msg}")
            self._log_error(error_msg)
            raise Exception(f"Failed to extract data: {error_msg}")

    def create_standard_query(self):
        """Create a standard query for non-incremental loads"""
        columns_aux = self.table_data['COLUMNS']
        if self.table_data.get('ID_COLUMN', '') != '':
            columns_aux = f"{self.table_data['ID_COLUMN']} as id," + self.table_data['COLUMNS']
        
        query = f"select {columns_aux} from {self.table_data.get('SOURCE_SCHEMA', 'CAN NOT FIND SCHEMA NAME')}.{self.table_data.get('SOURCE_TABLE', 'CAN NOT FIND TABLE NAME')} {self.table_data.get('JOIN_EXPR', '')} "
        self.logger.info(f"initialized query: {query}")
        
        if self.table_data.get('FILTER_EXP', '').strip() != '' or self.table_data.get('FILTER_COLUMN', '').strip() != '':
            if self.table_data['LOAD_TYPE'] == 'full':
                FILTER_COLUMN = '0=0'
            else:
                lower_limit, upper_limit = self.get_limits_for_filter(
                    self.table_data.get('DELAY_INCREMENTAL_INI', -2), 
                    self.table_data.get('FILTER_DATA_TYPE', ""))
                FILTER_COLUMN = self.table_data['FILTER_COLUMN'].replace('{0}', lower_limit).replace('{1}', upper_limit)
                
            if self.table_data.get('FILTER_EXP', '').strip() != '':
                FILTER_EXP = self.table_data['FILTER_EXP']
            else:
                FILTER_EXP = '0=0'
                
            query += f'where {FILTER_EXP} AND {FILTER_COLUMN}'
            
        self.logger.info(f"final query : {query}")
        return query

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

    def _log_error(self, error_message):
        """Log error to DynamoDB"""
        try:
            process_id = f"DLB_{self.table_name.split('_')[0]}_{self.table_data['SOURCE_TABLE']}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}"
            log = {
                'PROCESS_ID': process_id,
                'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
                'PROJECT_NAME': self.config['PROJECT_NAME'],
                'FLOW_NAME': 'extract_bigmagic',
                'TASK_NAME': 'extract_table_bigmagic',
                'TASK_STATUS': 'error',
                'MESSAGE': error_message,
                'PROCESS_TYPE': 'D' if self.table_data['LOAD_TYPE'].strip() in ['incremental'] else 'F',
                'CONTEXT': f"{{server='[{self.endpoint_data['ENDPOINT_NAME']},{self.endpoint_data['SRC_SERVER_NAME']}]', user='{self.endpoint_data['SRC_DB_USERNAME']}', table='{self.table_data['SOURCE_TABLE']}'}}"
            }
            self.logs_table_db.put_item(log)
        except Exception as e:
            self.logger.error(f"Failed to log error: {str(e)}")
    
    def _log_success(self):
        """Log success to DynamoDB"""
        try:
            process_id = f"DLB_{self.table_name.split('_')[0]}_{self.table_data['SOURCE_TABLE']}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}"
            log = {
                'PROCESS_ID': process_id,
                'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
                'PROJECT_NAME': self.config['PROJECT_NAME'],
                'FLOW_NAME': 'extract_bigmagic',
                'TASK_NAME': 'extract_table_bigmagic',
                'TASK_STATUS': 'satisfactorio',
                'MESSAGE': '',
                'PROCESS_TYPE': 'D' if self.table_data['LOAD_TYPE'].strip() in ['incremental'] else 'F',
                'CONTEXT': f"{{server='[{self.endpoint_data['ENDPOINT_NAME']},{self.endpoint_data['SRC_SERVER_NAME']}]', user='{self.endpoint_data['SRC_DB_USERNAME']}', table='{self.table_data['SOURCE_TABLE']}'}}"
            }
            self.logs_table_db.put_item(log)
            self.logger.info("DynamoDB updated with success status")
        except Exception as e:
            self.logger.error(f"Failed to log success: {str(e)}")

# Main execution
args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'S3_RAW_PREFIX', 'ARN_TOPIC_SUCCESS', 'PROJECT_NAME', 'TEAM', 'DATA_SOURCE', 'ENVIRONMENT', 'REGION', 'DYNAMO_LOGS_TABLE', 'ARN_TOPIC_FAILED', 'TABLE_NAME', 'TABLE_CONFIG', 'DB_CONFIG', 'COLUMNS_CONFIG'])

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
    "TABLE_CONFIG": args["TABLE_CONFIG"],
    "DB_CONFIG": args["DB_CONFIG"],
    "COLUMNS_CONFIG": args["COLUMNS_CONFIG"],
}

logger = custom_logger(__name__)
logger.info("Starting data extraction process")

try:
    # Create extractor instance
    extractor = DataExtractor(config)
    
    # Run extraction
    success = extractor.extract_data()
    
    if success:
        logger.info(f"Successfully extracted data for table {config['TABLE_NAME']}") 
    else:
        logger.error(f"Failed to extract data for table {config['TABLE_NAME']}")
        
except Exception as e:
    logger.error(f"An error occurred during extraction: {str(e)}")
    raise e
