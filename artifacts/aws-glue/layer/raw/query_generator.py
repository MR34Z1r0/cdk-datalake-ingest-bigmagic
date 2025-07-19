# query_generator.py
import datetime as dt
import calendar
import pytz
from dateutil.relativedelta import relativedelta
from aje_libs.common.logger import custom_logger
from aje_libs.common.helpers.dynamodb_helper import DynamoDBHelper
from aje_libs.common.helpers.secrets_helper import SecretsHelper

class QueryGenerator:
    def __init__(self, config):
        self.config = config
        self.logger = custom_logger(__name__)
        self.TZ_LIMA = pytz.timezone('America/Lima')
        
        # Initialize DynamoDB helpers
        self.dynamo_config_table = self.config['DYNAMO_CONFIG_TABLE'].strip()
        self.dynamo_endpoint_table = self.config['DYNAMO_ENDPOINT_TABLE'].strip()
        
        self.config_table_db = DynamoDBHelper(self.dynamo_config_table, "TARGET_TABLE_NAME", None)
        self.endpoint_table_db = DynamoDBHelper(self.dynamo_endpoint_table, "ENDPOINT_NAME", None)
    
    def generate_queries_from_table(self, table_name, fecha_ini=None, fecha_fin=None):
        """
        Genera queries y metadatos basado en el nombre de la tabla
        Returns: dict con queries o queries pendientes para ejecutar
        """
        try:
            self.logger.info(f"Generating queries for table: {table_name}")
            
            # Get table and endpoint metadata
            table_data = self.config_table_db.get_item(table_name)
            endpoint_data = self.endpoint_table_db.get_item(table_data['ENDPOINT_NAME'])
            
            # Determine load strategy
            strategy_result = self._determine_load_strategy(table_data, endpoint_data, fecha_ini, fecha_fin)
            
            return {
                'strategy_result': strategy_result,
                'table_data': table_data,
                'endpoint_data': endpoint_data,
                'table_name': table_name
            }
            
        except Exception as e:
            self.logger.error(f"Error generating queries for table {table_name}: {str(e)}")
            raise
    
    def _determine_load_strategy(self, table_data, endpoint_data, fecha_ini=None, fecha_fin=None):
        """Determine the load strategy and return appropriate result"""
        load_type = table_data.get('LOAD_TYPE', '').strip().lower()
        table_type = table_data.get('SOURCE_TABLE_TYPE', '')
        partition_column = table_data.get('PARTITION_COLUMN', '').strip()
        
        self.logger.info(f"Load type: {load_type}, Table type: {table_type}, Partition column: {partition_column}")
        
        # Full load with partitioning - NEEDS MIN/MAX QUERY
        if load_type == 'full' and table_type == 't' and partition_column:
            min_max_query = self._generate_min_max_query(table_data, partition_column)
            
            return {
                'type': 'partitioned_full',
                'partition_column': partition_column,
                'min_max_query': min_max_query,
                'needs_execution': True,
                'next_step': 'execute_min_max_then_call_generate_partitioned_queries'
            }
        
        # Between-date load - DIRECT QUERIES
        elif load_type == 'between-date':
            start = table_data.get('START_VALUE', '').strip()
            end = table_data.get('END_VALUE', '').strip()
            
            if start and end:
                try:
                    number_threads = int(self.config.get('THREADS_FOR_INCREMENTAL_LOADS', 6))
                    queries = self._generate_between_date_queries(table_data, start, end, number_threads)
                    
                    return {
                        'type': 'between_date',
                        'queries': queries,
                        'needs_execution': False,
                        'total_queries': len(queries)
                    }
                except Exception as e:
                    self.logger.warning(f"Between-date strategy failed: {e}")
        
        # Incremental with automatic date range - DIRECT QUERIES
        elif load_type == 'incremental':
            try:
                number_threads = int(self.config.get('THREADS_FOR_INCREMENTAL_LOADS', 1))
                delay = table_data.get('DELAY_INCREMENTAL_INI', -2)
                filter_type = table_data.get('FILTER_DATA_TYPE', '')
                
                queries = self._generate_incremental_queries(table_data, delay, filter_type, number_threads)
                
                return {
                    'type': 'incremental_auto',
                    'queries': queries,
                    'needs_execution': False,
                    'total_queries': len(queries)
                }
            except Exception as e:
                self.logger.warning(f"Incremental strategy failed: {e}")
        
        # Default: Standard load - DIRECT QUERY
        query = self._create_standard_query(table_data, fecha_ini, fecha_fin)
        return {
            'type': 'standard',
            'queries': [{
                'query': query,
                'type': 'standard',
                'thread_id': 0
            }],
            'needs_execution': False,
            'total_queries': 1
        }
    
    def _generate_min_max_query(self, table_data, partition_column):
        """Generate min/max query for a partition column"""
        source_schema = table_data.get('SOURCE_SCHEMA', '')
        source_table = table_data.get('SOURCE_TABLE', '')
        
        min_max_query = f"SELECT MIN({partition_column}) as min_val, MAX({partition_column}) as max_val FROM {source_schema}.{source_table} {table_data.get('JOIN_EXPR', '')} WHERE {partition_column} <> 0"
        
        if table_data.get('FILTER_EXP', '').strip():
            min_max_query += f" AND {table_data['FILTER_EXP']}"
        
        self.logger.info(f"Generated Min/Max query: {min_max_query}")
        return min_max_query
    
    def generate_partitioned_queries(self, min_val, max_val, table_data, max_threads=30):
        """Generate partitioned queries once min/max values are known"""
        try:
            if min_val is None or max_val is None:
                raise ValueError("Min or Max values cannot be None")
            
            partition_column = table_data.get('PARTITION_COLUMN', '').strip()
            range_size = max_val - min_val
            number_threads = min(max_threads, max(1, range_size))
            increment = max(1, range_size // number_threads)
            
            self.logger.info(f"Generating partitioned queries: Min={min_val}, Max={max_val}, Threads={number_threads}, Increment={increment}")
            
            queries = []
            for i in range(number_threads):
                query = self._get_partitioned_query(
                    table_data,
                    partition_column,
                    min_val,
                    increment,
                    i,
                    number_threads
                )
                queries.append({
                    'query': query,
                    'type': 'partition',
                    'partition_index': i,
                    'thread_id': i,
                    'start_range': int(min_val + (increment * i)),
                    'end_range': int(min_val + (increment * (i + 1))) if i < number_threads - 1 else int(min_val + (increment * (i + 1))) + 1
                })
            
            return queries
            
        except Exception as e:
            self.logger.error(f"Error generating partitioned queries: {str(e)}")
            raise
    
    def _generate_between_date_queries(self, table_data, start, end, number_threads):
        """Generate date-range queries"""
        queries = []
        start_dt = self._transform_to_dt(start)
        end_dt = self._transform_to_dt(end)
        delta = (end_dt - start_dt) / number_threads
        
        for i in range(number_threads):
            thread_start = start_dt + delta * i
            thread_end = start_dt + delta * (i + 1)
            
            query = self._get_query_for_date_range(
                table_data,
                str(thread_start)[:19],
                str(thread_end)[:19]
            )
            queries.append({
                'query': query,
                'type': 'date_range',
                'start_date': str(thread_start)[:19],
                'end_date': str(thread_end)[:19],
                'thread_id': i
            })
        
        return queries
    
    def _generate_incremental_queries(self, table_data, delay, filter_type, number_threads):
        """Generate incremental queries with automatic date range"""
        lower_limit, upper_limit = self._get_limits_for_filter(delay, filter_type)
        queries = []
        
        for i in range(number_threads):
            query = self._create_incremental_query(table_data, lower_limit, upper_limit)
            queries.append({
                'query': query,
                'type': 'incremental',
                'thread_id': i,
                'lower_limit': lower_limit,
                'upper_limit': upper_limit
            })
        
        return queries
    
    def _get_partitioned_query(self, table_data, partition_column, min_val, increment, partition_index, num_partitions):
        """Generate partitioned query based on min/max range"""
        start_value = int(min_val + (increment * partition_index))
        
        if partition_index == num_partitions - 1:
            end_value = int(min_val + (increment * (partition_index + 1))) + 1
        else:
            end_value = int(min_val + (increment * (partition_index + 1)))
        
        columns_aux = table_data['COLUMNS']
        if table_data.get('ID_COLUMN', ''):
            columns_aux = f"{table_data['ID_COLUMN']} as id," + columns_aux
        
        query = f"SELECT {columns_aux} FROM {table_data.get('SOURCE_SCHEMA', '')}.{table_data.get('SOURCE_TABLE', '')} {table_data.get('JOIN_EXPR', '')} WHERE {partition_column} >= {start_value} AND {partition_column} < {end_value}"
        
        if table_data.get('FILTER_EXP', '').strip():
            query += f" AND ({table_data['FILTER_EXP']})"
        
        self.logger.info(f"Partitioned query {partition_index}: Range {start_value} to {end_value}")
        return query
    
    def _get_query_for_date_range(self, table_data, start, end):
        """Generate query for a specific date range"""
        columns_aux = table_data['COLUMNS']
        if table_data.get('ID_COLUMN', ''):
            columns_aux = f"{table_data['ID_COLUMN']} as id," + columns_aux
        
        query = f"SELECT {columns_aux} FROM {table_data.get('SOURCE_SCHEMA', '')}.{table_data.get('SOURCE_TABLE', '')} {table_data.get('JOIN_EXPR', '')}"
        
        # Format dates based on filter type
        if 'FILTER_TYPE' in table_data:
            start_formatted, end_formatted = self._change_date_format(start, end, table_data['FILTER_TYPE'])
        else:
            start_formatted, end_formatted = f"'{start}'", f"'{end}'"
        
        # Build date filter
        filter_column = table_data.get('FILTER_COLUMN', '')
        if ',' in filter_column:
            columns = filter_column.split(",")
            first_col = columns[0].strip()
            second_col = columns[1].strip()
            date_filter = f"(({first_col} IS NOT NULL AND {first_col} BETWEEN {start_formatted} AND {end_formatted}) OR ({second_col} IS NOT NULL AND {second_col} BETWEEN {start_formatted} AND {end_formatted}))"
        else:
            date_filter = f"{filter_column.strip()} IS NOT NULL AND {filter_column.strip()} BETWEEN {start_formatted} AND {end_formatted}"
        
        # Add WHERE clause
        filters = [date_filter]
        if table_data.get('FILTER_EXP', '').strip():
            filters.append(table_data['FILTER_EXP'])
        
        query += f" WHERE {' AND '.join(f'({f})' for f in filters)}"
        
        return query
    
    def _create_incremental_query(self, table_data, lower_limit, upper_limit):
        """Create incremental query with automatic date limits"""
        columns_aux = table_data['COLUMNS']
        if table_data.get('ID_COLUMN', ''):
            columns_aux = f"{table_data['ID_COLUMN']} as id," + columns_aux
        
        query = f"SELECT {columns_aux} FROM {table_data.get('SOURCE_SCHEMA', '')}.{table_data.get('SOURCE_TABLE', '')} {table_data.get('JOIN_EXPR', '')}"
        
        filters = []
        
        # Add filter column with limits
        if table_data.get('FILTER_COLUMN', '').strip():
            filter_column_expr = table_data['FILTER_COLUMN'].replace('{0}', str(lower_limit)).replace('{1}', str(upper_limit))
            filters.append(filter_column_expr)
        
        # Add additional filter expression
        if table_data.get('FILTER_EXP', '').strip():
            filters.append(table_data['FILTER_EXP'])
        
        if filters:
            query += f" WHERE {' AND '.join(f'({f})' for f in filters)}"
        
        return query
    
    def _create_standard_query(self, table_data, fecha_ini=None, fecha_fin=None):
        """Create standard query (full load or simple incremental)"""
        columns_aux = table_data['COLUMNS']
        if table_data.get('ID_COLUMN', ''):
            columns_aux = f"{table_data['ID_COLUMN']} as id," + columns_aux
        
        query = f"SELECT {columns_aux} FROM {table_data.get('SOURCE_SCHEMA', '')}.{table_data.get('SOURCE_TABLE', '')} {table_data.get('JOIN_EXPR', '')}"
        
        filters = []
        
        # Add custom date filters if provided
        if fecha_ini and fecha_fin and table_data.get('FILTER_COLUMN', ''):
            filter_type = table_data.get('FILTER_TYPE', '')
            if filter_type:
                start_formatted, end_formatted = self._change_date_format(fecha_ini, fecha_fin, filter_type)
            else:
                start_formatted, end_formatted = f"'{fecha_ini}'", f"'{fecha_fin}'"
            
            filter_column = table_data.get('FILTER_COLUMN', '')
            if ',' in filter_column:
                columns = filter_column.split(",")
                first_col = columns[0].strip()
                second_col = columns[1].strip()
                date_filter = f"(({first_col} IS NOT NULL AND {first_col} BETWEEN {start_formatted} AND {end_formatted}) OR ({second_col} IS NOT NULL AND {second_col} BETWEEN {start_formatted} AND {end_formatted}))"
            else:
                date_filter = f"{filter_column.strip()} IS NOT NULL AND {filter_column.strip()} BETWEEN {start_formatted} AND {end_formatted}"
            
            filters.append(date_filter)
        
        # Add table's own filters
        load_type = table_data.get('LOAD_TYPE', '').strip().lower()
        
        if load_type == 'incremental' and not (fecha_ini and fecha_fin):
            # Use automatic incremental limits
            delay = table_data.get('DELAY_INCREMENTAL_INI', -2)
            filter_type = table_data.get('FILTER_DATA_TYPE', '')
            lower_limit, upper_limit = self._get_limits_for_filter(delay, filter_type)
            
            if table_data.get('FILTER_COLUMN', ''):
                filter_column_expr = table_data['FILTER_COLUMN'].replace('{0}', str(lower_limit)).replace('{1}', str(upper_limit))
                filters.append(filter_column_expr)
        
        # Add general filter expression
        if table_data.get('FILTER_EXP', '').strip():
            filters.append(table_data['FILTER_EXP'])
        
        if filters:
            query += f" WHERE {' AND '.join(f'({f})' for f in filters)}"
        
        return query
    
    def _get_limits_for_filter(self, month_diff, data_type):
        """Get lower and upper limits for date filters based on data type"""
        data_type = data_type.strip()
        upper_limit = dt.datetime.now(self.TZ_LIMA)
        lower_limit = upper_limit - relativedelta(months=(-1*int(month_diff)))
        
        if data_type == "aje_period":
            return lower_limit.strftime('%Y%m'), upper_limit.strftime('%Y%m')
        
        elif data_type == "aje_date":
            _, last_day = calendar.monthrange(upper_limit.year, upper_limit.month)
            upper_limit = upper_limit.replace(day=last_day, tzinfo=self.TZ_LIMA)
            lower_limit = lower_limit.replace(day=1, tzinfo=self.TZ_LIMA)
            upper_limit = (upper_limit - dt.datetime(1900, 1, 1, tzinfo=self.TZ_LIMA)).days + 693596
            lower_limit = (lower_limit - dt.datetime(1900, 1, 1, tzinfo=self.TZ_LIMA)).days + 693596
            return str(lower_limit), str(upper_limit)
            
        elif data_type == "aje_processperiod":
            _, last_day = calendar.monthrange(upper_limit.year, upper_limit.month)
            upper_limit = upper_limit.replace(day=last_day, tzinfo=self.TZ_LIMA)
            lower_limit = lower_limit.replace(day=1, tzinfo=self.TZ_LIMA)
            upper_limit = (upper_limit - dt.datetime(1900, 1, 1, tzinfo=self.TZ_LIMA)).days + 693596
            lower_limit = (lower_limit - dt.datetime(1900, 1, 1, tzinfo=self.TZ_LIMA)).days + 693596
            return str(lower_limit), str(upper_limit)
      
        return lower_limit.strftime('%Y%m'), upper_limit.strftime('%Y%m')
    
    def _change_date_format(self, start, end, date_type):
        """Change date format based on database type"""
        if date_type == 'smalldatetime':
            start_formatted = f"CONVERT(smalldatetime, '{start}', 120)"
            end_formatted = f"CONVERT(smalldatetime, '{end}', 120)"
        
        elif date_type == 'DATE':
            start_formatted = f"TO_DATE('{start[:19]}', 'YYYY-MM-DD HH24:MI:SS')"
            end_formatted = f"TO_DATE('{end[:19]}', 'YYYY-MM-DD HH24:MI:SS')"
        
        elif date_type == 'TIMESTAMP(6)':
            start_formatted = f"TO_TIMESTAMP('{start}', 'YYYY-MM-DD HH24:MI:SS.FF')"
            end_formatted = f"TO_TIMESTAMP('{end}', 'YYYY-MM-DD HH24:MI:SS.FF')"
        
        elif date_type == 'SQL_DATETIME':
            start_formatted = f"CONVERT(DATETIME, '{start}', 102)"
            end_formatted = f"CONVERT(DATETIME, '{end}', 102)"
        
        elif date_type == 'BIGINT':
            start_dt = dt.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
            end_dt = dt.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
            start_formatted = str(int(start_dt.timestamp()))
            end_formatted = str(int(end_dt.timestamp()))
        
        else:
            start_formatted = f"'{start}'"
            end_formatted = f"'{end}'"
        
        return start_formatted, end_formatted
    
    def _transform_to_dt(self, date_str):
        """Convert string date to datetime object"""
        return dt.datetime(
            year=int(date_str[:4]),
            month=int(date_str[5:7]),
            day=int(date_str[8:10]),
            hour=int(date_str[11:13]),
            minute=int(date_str[14:16]),
            second=int(date_str[17:19])
        )