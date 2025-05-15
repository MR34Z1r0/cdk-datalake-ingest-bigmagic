# Import required modules
import concurrent.futures as futures
import datetime as dt
import calendar
import json
import logging
import os
import sys
import time
import argparse
import csv
import gzip
import subprocess
from io import StringIO

import boto3
import pytz
import pandas as pd
import awswrangler as wr
from dateutil.relativedelta import relativedelta

# Setup timezone and date variables
TZ_LIMA = pytz.timezone('America/Lima')
YEARS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%Y')
MONTHS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%m')
DAYS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%d')
NOW_LIMA = dt.datetime.now(pytz.utc).astimezone(TZ_LIMA)

class DataExtractor:
    def __init__(self, config):
        self.config = config
        self.logger = self._setup_logger()
        self.dynamodb = boto3.resource('dynamodb')
        
        # Initialize DynamoDB tables
        self.dynamo_config_table = self.config['DYNAMO_CONFIG_TABLE'].strip()
        self.dynamo_endpoint_table = self.config['DYNAMO_ENDPOINT_TABLE'].strip()
        self.dynamo_logs_table = self.config['DYNAMO_LOGS_TABLE'].strip()
        self.secrets_region = self.config['SECRETS_REGION'].strip()
        
        # Initialize table data and endpoint data
        self._initialize_data()
        
    def _setup_logger(self):
        """Setup logger configuration"""
        logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s")
        logger = logging.getLogger("raw job")
        logger.setLevel(os.environ.get("LOGGING", logging.INFO))
        return logger

    def _initialize_data(self):
        """Initialize table and endpoint data from DynamoDB"""
        try:
            self.config_table_metadata = self.dynamodb.Table(self.dynamo_config_table)
            self.endpoint_table_metadata = self.dynamodb.Table(self.dynamo_endpoint_table)
            self.s3_source = self.config['S3_RAW_PREFIX'].strip()
            self.table_name = self.config['TABLE_NAME'].strip()
            
            # Get table metadata
            self.table_data = self.config_table_metadata.get_item(Key={'TARGET_TABLE_NAME': self.table_name})['Item']
            
            # Get endpoint metadata
            self.endpoint_data = self.endpoint_table_metadata.get_item(
                Key={'ENDPOINT_NAME': self.table_data['ENDPOINT']})['Item']
            
            # Set S3 path
            self.day_route = f"{self.config['PROJECT_NAME']}/{self.config['DATA_SOURCE']}/{self.table_data['ENDPOINT']}/{self.table_data['SOURCE_TABLE'].split()[0]}/year={YEARS_LIMA}/month={MONTHS_LIMA}/day={DAYS_LIMA}/"
            self.s3_raw_path = self.s3_source + self.day_route
            self.bucket = self.s3_source.split("/")[2]
            
            # Set up connection details based on DB type
            if self.endpoint_data['BD_TYPE'] == 'oracle':
                self.url = f"{self.endpoint_data['SRC_SERVER_NAME']}:{self.endpoint_data['DB_PORT_NUMBER']}/{self.endpoint_data['SRC_DB_NAME']}"
                self.driver = "oracle.jdbc.driver.OracleDriver"
            elif self.endpoint_data['BD_TYPE'] == 'mssql':
                self.url = f"{self.endpoint_data['SRC_SERVER_NAME']}:{self.endpoint_data['DB_PORT_NUMBER']}"
                self.driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            elif self.endpoint_data['BD_TYPE'] == 'mysql':
                self.url = f"{self.endpoint_data['SRC_SERVER_NAME']}:{self.endpoint_data['DB_PORT_NUMBER']}"
                self.driver = "com.mysql.cj.jdbc.Driver"
                
            self.logger.info(f"driver: {self.driver}")
            self.logger.info(f"url: {self.url}")
            
        except Exception as e:
            self.logger.error("Error while searching for table data")
            self.logger.error(e)
            self._log_error(str(e))
            raise Exception(f"Failed to initialize data: {str(e)}")
    
    def _log_error(self, error_message):
        """Log error to DynamoDB and update table status"""
        try:
            # Update status in DynamoDB config table
            self.update_attribute_value_dynamodb(
                'TARGET_TABLE_NAME', self.table_data['TARGET_TABLE_NAME'], 'STATUS_RAW', 'FAILED', self.dynamo_config_table)
            self.update_attribute_value_dynamodb(
                'TARGET_TABLE_NAME', self.table_data['TARGET_TABLE_NAME'], 'STATUS_STAGE', 'FAILED', self.dynamo_config_table)
            self.update_attribute_value_dynamodb(
                'TARGET_TABLE_NAME', self.table_data['TARGET_TABLE_NAME'], 'FAIL REASON', error_message, self.dynamo_config_table)
            
            # Send error message via SNS
            self.send_error_message(self.config['TOPIC_ARN'], self.table_data['TARGET_TABLE_NAME'], error_message)
            
            # Add log entry to DynamoDB logs table
            log = {
                'PROCESS_ID': f"DLB_{self.table_name.split('_')[0]}_{self.table_data['SOURCE_TABLE']}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
                'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
                'PROJECT_NAME': self.config['PROJECT_NAME'],
                'FLOW_NAME': 'extract_bigmagic',
                'TASK_NAME': 'extract_table_bigmagic',
                'TASK_STATUS': 'error',
                'MESSAGE': error_message,
                'PROCESS_TYPE': 'D' if self.table_data['LOAD_TYPE'].strip() in ['incremental'] else 'F',
                'CONTEXT': f"{{server='[{self.endpoint_data['ENDPOINT_NAME']},{self.endpoint_data['SRC_SERVER_NAME']}]', user='{self.endpoint_data['SRC_DB_USERNAME']}', table='{self.table_data['SOURCE_TABLE']}'}}"
            }
            self.add_log_to_dynamodb(self.dynamo_logs_table, log)
        except Exception as e:
            self.logger.error(f"Failed to log error: {str(e)}")
    
    def _log_success(self):
        """Log success to DynamoDB and update table status"""
        try:
            # Update status in DynamoDB config table
            self.update_attribute_value_dynamodb(
                'TARGET_TABLE_NAME', self.table_data['TARGET_TABLE_NAME'], 'STATUS_RAW', 'SUCCEDED', self.dynamo_config_table)
            self.update_attribute_value_dynamodb(
                'TARGET_TABLE_NAME', self.table_data['TARGET_TABLE_NAME'], 'FAIL REASON', "", self.dynamo_config_table)
            
            # Add log entry to DynamoDB logs table
            log = {
                'PROCESS_ID': f"DLB_{self.table_name.split('_')[0]}_{self.table_data['SOURCE_TABLE']}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
                'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
                'PROJECT_NAME': self.config['PROJECT_NAME'],
                'FLOW_NAME': 'extract_bigmagic',
                'TASK_NAME': 'extract_table_bigmagic',
                'TASK_STATUS': 'satisfactorio',
                'MESSAGE': '',
                'PROCESS_TYPE': 'D' if self.table_data['LOAD_TYPE'].strip() in ['incremental'] else 'F',
                'CONTEXT': f"{{server='[{self.endpoint_data['ENDPOINT_NAME']},{self.endpoint_data['SRC_SERVER_NAME']}]', user='{self.endpoint_data['SRC_DB_USERNAME']}', table='{self.table_data['SOURCE_TABLE']}'}}"
            }
            self.add_log_to_dynamodb(self.dynamo_logs_table, log)
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
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket)
        bucket.objects.filter(Prefix=s3_raw_path).delete()

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

    def get_secret(self, secret_name, target_secret, secrets_region):
        """Get secret from AWS Secrets Manager"""
        region_name = secrets_region[:-1] 
        client = boto3.client(
            service_name='secretsmanager',
            region_name=region_name
        )
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )

            if 'SecretString' in get_secret_value_response:
                text_secret_data = get_secret_value_response['SecretString']
            else:
                text_secret_data = get_secret_value_response['SecretBinary']

            secret_json = json.loads(text_secret_data)
            return secret_json[target_secret]
        
        except Exception as e:
            self.logger.error("Exception thrown in get_secret: %s" % str(e))
            raise

    def update_attribute_value_dynamodb(self, row_key_field_name, row_key, attribute_name, attribute_value, table_name):
        """Update attribute value in DynamoDB table"""
        self.logger.info('update dynamoDb Metadata : {} ,{},{},{},{}'.format(
            row_key_field_name, row_key, attribute_name, attribute_value, table_name))
        dynamo_table = self.dynamodb.Table(table_name)
        response = dynamo_table.update_item(
            Key={row_key_field_name: row_key},
            AttributeUpdates={
                attribute_name: {
                    'Value': attribute_value,
                    'Action': 'PUT'
                }
            }
        )
        return response

    def add_log_to_dynamodb(self, table_name, record):
        """Add log record to DynamoDB table"""
        dynamo_table = self.dynamodb.Table(table_name)
        response = dynamo_table.put_item(Item=record)
        return response

    def execute_db_query(self, query, user, password):
        """Execute query on the database and return results as DataFrame"""
        try:
            # Connect to database based on database type
            if self.endpoint_data['BD_TYPE'] == 'oracle':
                import cx_Oracle
                conn = cx_Oracle.connect(user=user, password=password, dsn=self.url)
                df = pd.read_sql(query, conn)
                conn.close()
            
            elif self.endpoint_data['BD_TYPE'] == 'mssql':
                import pyodbc
                cnx_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.endpoint_data["SRC_SERVER_NAME"]}; DATABASE={self.endpoint_data["SRC_DB_NAME"]};UID={user};PWD={password}'
                conn = pyodbc.connect(cnx_str)
                df = pd.read_sql(query, conn)
                conn.close()
            
            elif self.endpoint_data['BD_TYPE'] == 'mysql':
                import pymysql
                conn = pymysql.connect(host=self.endpoint_data['SRC_SERVER_NAME'], 
                                    user=user,
                                    password=password,
                                    database=self.endpoint_data['SRC_DB_NAME'])
                df = pd.read_sql(query, conn)
                conn.close()
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error executing database query: {str(e)}")
            raise

    def get_data(self, query, s3_raw_path, actual_thread, number_threads):
        """Get data from database and write to S3"""
        try:
            self.logger.info(query)
            
            # Get database credentials
            user = self.endpoint_data['SRC_DB_USERNAME']
            password = self.get_secret(self.config['SECRETS_NAME'], self.endpoint_data["SRC_DB_SECRET"], self.secrets_region)
            
            # Execute query
            df = self.execute_db_query(query, user, password)
            
            # Drop duplicates
            df = df.drop_duplicates()
            
            # Write to S3 using AWS Wrangler
            if len(df) == 0:
                # For empty dataframes, write a single file
                wr.s3.to_csv(df, path=s3_raw_path + "empty_data.csv.gz", index=False, compression='gzip')
            else:
                # For non-empty dataframes, distribute the write
                wr.s3.to_csv(df, path=s3_raw_path, index=False, compression='gzip', dataset=True)
            
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
            
            # Get credentials
            user = self.endpoint_data['SRC_DB_USERNAME']
            password = self.get_secret(self.config['SECRETS_NAME'], self.endpoint_data["SRC_DB_SECRET"], self.secrets_region)
            
            # Execute query
            df_min_max = self.execute_db_query(min_max_query, user, password)
            
            # Get min and max values from the DataFrame and convert to integers
            min_val = int(df_min_max['min_val'].iloc[0])
            max_val = int(df_min_max['max_val'].iloc[0])
            
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
        columns_aux = self.table_data['COLUMNS']
        if self.table_data.get('ID_COLUMN', '') != '':
            columns_aux = f"{self.table_data['ID_COLUMN']} as id," + self.table_data['COLUMNS']
        
        query = f"select {columns_aux} from {self.table_data.get('SOURCE_SCHEMA', 'CAN NOT FIND SCHEMA NAME')}.{self.table_data.get('SOURCE_TABLE', 'CAN NOT FIND TABLE NAME')} {self.table_data.get('JOIN_EXPR', '')} "
        self.logger.info(f"initialized query: {query}")
        
        if self.table_data.get('FILTER_EXP', '').strip() != '' or self.table_data.get('FILTER_COLUMN', '').strip() != '':
            if self.table_data['LOAD_TYPE'] == 'full':
                self.logger.info(f"filter full")
                FILTER_COLUMN = '0=0'
            else:
                self.logger.info(f"filter incremental")
                lower_limit, upper_limit = self.get_limits_for_filter(
                    self.table_data.get('DELAY_INCREMENTAL_INI', -2), 
                    self.table_data.get('FILTER_DATA_TYPE', ""))
                FILTER_COLUMN = self.table_data['FILTER_COLUMN'].replace('{0}', lower_limit).replace('{1}', upper_limit)
                
            if self.table_data.get('FILTER_EXP', '').strip() != '':
                self.logger.info(f"add filter expression")
                FILTER_EXP = self.table_data['FILTER_EXP']
            else:
                self.logger.info(f"no filter expression")
                FILTER_EXP = '0=0'
                
            query += f'where {FILTER_EXP} AND {FILTER_COLUMN}'
            
        self.logger.info(f"final query : {query}")
        return query
    
    def determine_load_strategy(self):
        """Determine the load strategy based on table configuration"""
        # Initialize default values
        number_threads = 1
        incremental_load = False
        
        # Check if it's a full load with partitioning
        if (self.table_data['LOAD_TYPE'].strip() == 'full' and 
            self.table_data.get('SOURCE_TABLE_TYPE', '') == 't' and 
            self.table_data.get('PARTITION_COLUMN', '').strip() != ''):
            
            self.logger.info("Full load with partitioning based on min/max range")
            
            # Get partition column
            partition_column = self.table_data['PARTITION_COLUMN']
            
            # Get min and max values
            min_val, max_val = self.get_min_max_values(partition_column)
            
            # Calculate range and increment - ensure we work with integers
            range_size = max_val - min_val
            number_threads = 30  # Fixed at 30 partitions as requested
            
            # If range is too small, adjust number of threads
            if range_size < number_threads:
                number_threads = max(1, range_size)
                self.logger.info(f"Adjusted number of partitions to {number_threads} because range is {range_size}")
            
            # Calculate increment - ensure it's at least 1 for integer partitioning
            increment = max(1, range_size // number_threads)
            
            self.logger.info(f"Partition column: {partition_column}, Min: {min_val}, Max: {max_val}, "
                            f"Range: {range_size}, Increment: {increment}, Partitions: {number_threads}")
            
            # Set incremental load flag and return parameters
            incremental_load = True
            return {
                'load_type': 'partitioned_full',
                'number_threads': number_threads,
                'incremental_load': incremental_load,
                'partition_column': partition_column,
                'min_val': min_val,
                'max_val': max_val,
                'increment': increment
            }
            
        # Check if it's a between-date load
        elif self.table_data['LOAD_TYPE'].strip() in ['between-date']:
            self.logger.info("Incremental load with date range")
            
            end = self.table_data['END_VALUE'].strip()
            start = self.table_data['START_VALUE'].strip()
            
            self.logger.info(f"end: {end}")
            self.logger.info(f"start: {start}")
            
            number_threads = int(self.config['THREADS_FOR_INCREMENTAL_LOADS'])
            start_dt = self.transform_to_dt(start)
            end_dt = self.transform_to_dt(end)
            
            delta = (end_dt - start_dt) / number_threads
            
            incremental_load = True
            return {
                'load_type': 'between_date',
                'number_threads': number_threads,
                'incremental_load': incremental_load,
                'start_dt': start_dt,
                'end_dt': end_dt,
                'delta': delta
            }
            
        # Default to standard load
        else:
            self.logger.info("Standard load")
            return {
                'load_type': 'standard',
                'number_threads': number_threads,
                'incremental_load': incremental_load
            }
    
    def extract_data(self):
        """Main method to extract data from the source and load to S3"""
        try:
            # Delete existing data in the target S3 path
            self.delete_from_target(self.bucket, self.day_route)
            
            # Determine load strategy
            load_strategy = self.determine_load_strategy()
            number_threads = load_strategy['number_threads']
            incremental_load = load_strategy['incremental_load']
            
            # Initialize thread pool
            futures_executor = []
            actual_thread = 0
            
            with futures.ThreadPoolExecutor(max_workers=number_threads + 1) as executor:
                self.logger.info("Starting to configure connections")
                
                # Process each partition with a separate thread
                for i in range(number_threads):
                    self.logger.info(f"Configuring thread {i+1}")
                    
                    # Generate the appropriate query based on load strategy
                    if incremental_load:
                        if load_strategy['load_type'] == 'partitioned_full':
                            # Generate partitioned query for full load with partitioning
                            query = self.get_partitioned_query(
                                load_strategy['partition_column'],
                                load_strategy['min_val'],
                                load_strategy['increment'],
                                i,
                                number_threads
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
                    
                    # Submit task to thread pool
                    futures_executor.append(
                        executor.submit(
                            self.get_data,
                            query,
                            self.s3_raw_path,
                            actual_thread,
                            number_threads
                        )
                    )
                    actual_thread += 1
            
            # Monitor thread completion
            if incremental_load:
                threads_running = True
                total_processed_tables = 0
                
                while threads_running:
                    for future in list(futures_executor):
                        if not future.running():
                            total_processed_tables += 1
                            futures_executor.remove(future)
                            self.logger.info("Data loaded in the job")
                            self.logger.info(f"{total_processed_tables}/{number_threads}")
                    
                    if len(futures_executor) == 0:
                        threads_running = False
                    
                    time.sleep(5)
            else:
                futures.wait(futures_executor)
                result = futures_executor[0].result()
            
            self.logger.info("Data loaded to S3 successfully")
            
            # Log success
            self._log_success()
            
            return True
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Error in extract_data: {error_msg}")
            self._log_error(error_msg)
            raise Exception(f"Failed to extract data: {error_msg}")

def main():
    """Main function to set up and run the data extraction"""
    # Set up AWS session
    region_name = 'us-east-1'
    boto3.setup_default_session(profile_name='prd-valorx-admin', region_name=region_name)
    
    # Default configuration
    config = {
        "S3_RAW_PREFIX": "s3://sofia-566121885938-us-east-1-dev-datalake-raw-s3/",
        "DYNAMO_CONFIG_TABLE": "sofia-dev-datalake-configuration-ddb",
        "DYNAMO_ENDPOINT_TABLE": "sofia-dev-datalake-credentials-ddb",
        "DYNAMO_LOGS_TABLE": "sofia-dev-datalake-logs-ddb",
        "SECRETS_NAME": "BDDATA_PWD",
        "THREADS_FOR_INCREMENTAL_LOADS": "6",
        "TOPIC_ARN": "arn:aws:sns:us-east-1:566121885938:sofia-dev-datalake-failed-sns",
        "SECRETS_REGION": "us-east-1a",
        "PROJECT_NAME": "sofia",
        "DATA_SOURCE": "bigmagic"
    }
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract data from source and load to S3')
    parser.add_argument("-t", '--TABLE_NAME', required=True, help='Target table name')
    args = parser.parse_args()
    
    # Update config with table name from arguments
    config["TABLE_NAME"] = args.TABLE_NAME
    
    try:
        # Create extractor instance
        extractor = DataExtractor(config)
        
        # Run extraction
        success = extractor.extract_data()
        
        if success:
            print(f"Successfully extracted data for table {args.TABLE_NAME}")
            return 0
        else:
            print(f"Failed to extract data for table {args.TABLE_NAME}")
            return 1
            
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())