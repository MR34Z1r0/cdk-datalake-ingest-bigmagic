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
from io import StringIO
from dateutil.relativedelta import relativedelta
import uuid
import boto3
import pytz
import pandas as pd
import awswrangler as wr

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
        self.dynamo_config_table = self.config['DYNAMO_CONFIG_TABLE'].strip()
        self.dynamo_endpoint_table = self.config['DYNAMO_ENDPOINT_TABLE'].strip()
        self.dynamo_logs_table = self.config['DYNAMO_LOGS_TABLE'].strip()
        self.secrets_region = self.config['SECRETS_REGION'].strip()
        
        # Initialize DynamoDB helpers with respective tables
        self.config_table_db = DynamoDBHelper(self.dynamo_config_table, "TARGET_TABLE_NAME", None)
        self.endpoint_table_db = DynamoDBHelper(self.dynamo_endpoint_table, "ENDPOINT_NAME", None)
        self.logs_table_db = DynamoDBHelper(self.dynamo_logs_table, "PROCESS_ID", None)
        
        # Initialize S3 helper
        self.s3_bucket = self.config['S3_RAW_PREFIX'].strip().split("/")[2]
        self.s3_helper = S3Helper(self.s3_bucket)
        
        # Initialize table data and endpoint data
        self._initialize_data()
        
    def _initialize_data(self):
        """Initialize table and endpoint data from DynamoDB"""
        try:
            self.s3_source = self.config['S3_RAW_PREFIX'].strip()
            self.table_name = self.config['TABLE_NAME'].strip()
            
            # Get table metadata using DynamoDBHelper
            self.table_data = self.config_table_db.get_item(self.table_name)
            
            # Get endpoint metadata using DynamoDBHelper
            self.endpoint_data = self.endpoint_table_db.get_item(self.table_data['ENDPOINT'])
            
            # Set S3 path
            self.day_route = f"{self.config['PROJECT_NAME']}/{self.config['DATA_SOURCE']}/{self.table_data['ENDPOINT']}/{self.table_data['SOURCE_TABLE'].split()[0]}/year={YEARS_LIMA}/month={MONTHS_LIMA}/day={DAYS_LIMA}/"
            self.s3_raw_path = self.s3_source + self.day_route
            self.bucket = self.s3_source.split("/")[2]
            
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
            self.secrets_helper = SecretsHelper(self.config['SECRETS_NAME'])
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
    
    def _log_error(self, error_message):
        """Log error to DynamoDB and update table status"""
        try:
            # Update status in DynamoDB config table
            self.config_table_db.update_item(
                partition_key=self.table_data['TARGET_TABLE_NAME'],
                update_expression="SET STATUS_RAW = :raw, STATUS_STAGE = :stage, #reason = :reason",
                expression_attribute_values={
                    ":raw": "FAILED",
                    ":stage": "FAILED",
                    ":reason": error_message
                },
                expression_attribute_names={
                    "#reason": "FAIL REASON"
                }
            )
            # Send error message via SNS
            self.send_error_message(self.config['TOPIC_ARN'], self.table_data['TARGET_TABLE_NAME'], error_message)
            
            # Add log entry to DynamoDB logs table
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
        """Log success to DynamoDB and update table status"""
        try:
            # Update status in DynamoDB config table
            self.config_table_db.update_item(
                partition_key=self.table_data['TARGET_TABLE_NAME'],
                update_expression="SET STATUS_RAW = :raw, STATUS_STAGE = :stage",
                expression_attribute_values={
                    ":raw": "SUCCEDED",
                    ":stage": ""
                }
            )
            # Add log entry to DynamoDB logs table
            process_id = f"DLB_{self.table_name.split('_')[0]}_{self.table_data['SOURCE_TABLE']}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}"
            self.logger.info(f"process_id: {process_id}")
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
            self.logger.info(query)
            
            # Execute query
            df = self.execute_db_query(query)
            
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
        number_threads = 1
        incremental_load = False

        load_type = self.table_data.get('LOAD_TYPE', '').strip().lower()
        table_type = self.table_data.get('SOURCE_TABLE_TYPE', '')
        partition_column = self.table_data.get('PARTITION_COLUMN', '').strip()

        # Full load with partitioning
        if load_type == 'full' and table_type == 't' and partition_column:
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

def main():
    
    """Main function to set up and run the data extraction"""
    # Set up AWS session
    region_name = 'us-east-1'
    boto3.setup_default_session(profile_name='prd-valorx-admin', region_name=region_name)
    
    today = dt.datetime.now().strftime("%Y-%m-%d")
    time = dt.datetime.now().strftime("%H%M%S")
    log_dir = os.path.join("logs", today)
    correlation_id = str(uuid.uuid4())
    log_file = f"{log_dir}/data_extractor.log"
    set_logger_config(
        log_level=logging.INFO,
        log_file=log_file,
        service="data_extractor",
        correlation_id=correlation_id
    )
    
    logger = custom_logger(__name__)
    
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
        logger.info("Creating extractor instance")
        extractor = DataExtractor(config)
        logger.info("Extractor instance created")
        # Run extraction
        success = extractor.extract_data()
        logger.info("Extraction completed")
        if success:
            logger.info(f"Successfully extracted data for table {args.TABLE_NAME}")
            return 0
        else:
            logger.error(f"Failed to extract data for table {args.TABLE_NAME}")
            return 1
            
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())