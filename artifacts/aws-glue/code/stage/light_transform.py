import datetime as dt
import logging
import os
import sys
import time
import json
import csv
from io import StringIO
import boto3
import pytz
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from dateutil.relativedelta import relativedelta
from py4j.protocol import Py4JJavaError
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, expr, lpad, row_number
from pyspark.sql import Window
from pyspark.sql.functions import col, date_add, to_date, concat_ws, when, regexp_extract, trim, to_timestamp, lit, date_format, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType, DateType
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("LightTransform")
logger.setLevel(os.environ.get("LOGGING", logging.INFO))

args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'S3_RAW_BUCKET', 'S3_STAGE_BUCKET', 'DYNAMO_LOGS_TABLE', 'TABLE_NAME', 'ARN_TOPIC_FAILED', 'PROJECT_NAME', 'TEAM', 'DATA_SOURCE', 'TABLES_CSV_S3', 'CREDENTIALS_CSV_S3', 'COLUMNS_CSV_S3', 'ENDPOINT_NAME', 'ENVIRONMENT'])

# Load configuration from CSV files in S3
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

def clean_quotes_from_data(data):
    """Clean triple quotes and surrounding quotes from CSV data"""
    if isinstance(data, list):
        # If it's a list of dictionaries (like CSV data)
        for row in data:
            if isinstance(row, dict):
                for key in row.keys():
                    # Only if column is string
                    if isinstance(row[key], str):
                        # Clean triple quotes first
                        row[key] = row[key].replace('"""', '"')
                        # Clean surrounding quotes
                        if row[key].startswith('"'):
                            row[key] = row[key][1:]
                        if row[key].endswith('"'):
                            row[key] = row[key][0:-1]
    elif isinstance(data, dict):
        # If it's a single dictionary
        for key in data.keys():
            # Only if column is string
            if isinstance(data[key], str):
                # Clean triple quotes first
                data[key] = data[key].replace('"""', '"')
                # Clean surrounding quotes
                if data[key].startswith('"'):
                    data[key] = data[key][1:]
                if data[key].endswith('"'):
                    data[key] = data[key][0:-1]
    return data

def get_column_value(column, field_name):
    """Safely get column value from either DynamoDB format or CSV format"""
    field_value = column.get(field_name, {})
    if isinstance(field_value, dict):
        # DynamoDB format: {'S': 'value'} or {'N': '123'} or {'BOOL': True}
        if 'S' in field_value:
            return field_value['S']
        elif 'N' in field_value:
            return field_value['N']
        elif 'BOOL' in field_value:
            return field_value['BOOL']
        else:
            return field_value
    else:
        # CSV format: direct value
        return field_value

def get_clean_table_name(table_data, table_name):
    """
    Extract clean table name from SOURCE_TABLE, removing alias after space.
    For Ingest Bigmagic, the structure is: [sourcetablename] [alias] (alias is optional)
    
    Examples:
    - "mcompa1f m" -> "mcompa1f"
    - "users u" -> "users"  
    - "simple_table" -> "simple_table"
    """
    source_table = table_data.get('SOURCE_TABLE', table_name)
    # Split by space and take only the first part (table name)
    clean_name = source_table.split()[0] if source_table and ' ' in source_table else source_table
    logger.info(f"Clean table name extracted: '{source_table}' -> '{clean_name}'")
    return clean_name

# Load CSV data
tables_data = load_csv_from_s3(args['TABLES_CSV_S3'])
credentials_data = load_csv_from_s3(args['CREDENTIALS_CSV_S3'])
columns_data = load_csv_from_s3(args['COLUMNS_CSV_S3'])

# Clean quotes from all loaded data
tables_data = clean_quotes_from_data(tables_data)
credentials_data = clean_quotes_from_data(credentials_data)
columns_data = clean_quotes_from_data(columns_data)

# Filter data for current table and database
table_name = args['TABLE_NAME']
endpoint_name = args['ENDPOINT_NAME']
environment = args['ENVIRONMENT']

# Find table configuration
table_data = None
for row in tables_data:
    if row.get('STAGE_TABLE_NAME', '').upper() == table_name.upper():
        table_data = row
        break

if not table_data:
    raise Exception(f"Table configuration not found for {table_name}")

# Find database credentials
endpoint_data = None
for row in credentials_data:
    if (row.get('ENDPOINT_NAME', '') == endpoint_name and 
        row.get('ENV', '').upper() == environment.upper()):
        endpoint_data = row
        break

if not endpoint_data:
    raise Exception(f"EndPoint credentials not found for {endpoint_name} in {environment}")

# Add ENDPOINT_NAME for compatibility
endpoint_data['ENDPOINT_NAME'] = endpoint_data.get('ENDPOINT_NAME', '')

# Apply old logic to determine LOAD_TYPE if not explicitly set
if not table_data.get('LOAD_TYPE') or table_data.get('LOAD_TYPE', '').strip() == '':
    if table_data.get('SOURCE_TABLE_TYPE', '') == 't':
        if endpoint_data.get('ENDPOINT_NAME', '') == 'SALESFORCE_ING':
            table_data['LOAD_TYPE'] = 'days_off'
            table_data['NUM_DAYS'] = '10'
        else:
            table_data['LOAD_TYPE'] = 'incremental'
    else:
        table_data['LOAD_TYPE'] = 'full'

logger.info(f"Determined LOAD_TYPE: {table_data.get('LOAD_TYPE', 'not set')} for table {table_name}")

# Filter columns for current table
columns_metadata = {'Items': []}
for row in columns_data:
    if row.get('TABLE_NAME', '').upper() == table_name.upper():
        logger.info(f"Processing column {row.get('COLUMN_NAME', '')} with IS_PARTITION raw value: '{row.get('IS_PARTITION', 'false')}'")
        # Convert to expected format
        def convert_bool_field(value):
            """Convert string boolean to DynamoDB boolean format"""
            if isinstance(value, str):
                result = {'BOOL': value.lower() in ['true', '1', 'yes', 'y', 't']}
                logger.info(f"Converting boolean field: '{value}' -> {result}")
                return result
            return {'BOOL': bool(value)}
        
        column_item = {
            'COLUMN_NAME': {'S': row.get('COLUMN_NAME', '')},
            'COLUMN_ID': {'N': row.get('COLUMN_ID', '0')},
            'NEW_DATA_TYPE': {'S': row.get('NEW_DATA_TYPE', 'string')},
            'TRANSFORMATION': {'S': row.get('TRANSFORMATION', '')},
            'IS_PARTITION': convert_bool_field(row.get('IS_PARTITION', 'false')),
            'IS_ID': convert_bool_field(row.get('IS_ID', 'false')),
            'IS_ORDER_BY': convert_bool_field(row.get('IS_ORDER_BY', 'false')),
            'IS_FILTER_DATE': convert_bool_field(row.get('IS_FILTER_DATE', 'false'))
        }
        columns_metadata['Items'].append(column_item)

# logger.info(f"Loaded table config: {table_data}")
# logger.info(f"Loaded DB config keys: {list(endpoint_data.keys())}")
logger.info(f"Loaded columns count: {len(columns_metadata['Items'])}")

TZ_LIMA = pytz.timezone('America/Lima')
YEARS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%Y')
MONTHS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%m')
DAYS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%d')
NOW_LIMA = dt.datetime.now(pytz.utc).astimezone(TZ_LIMA)

spark = SparkSession \
    .builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .getOrCreate()

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')
dynamo_logs_table = args['DYNAMO_LOGS_TABLE'].strip()

s3_source = args['S3_RAW_BUCKET']
s3_target = args['S3_STAGE_BUCKET']
table_name = args['TABLE_NAME']
project_name = args['PROJECT_NAME']

class NoDataToMigrateException(Exception):
    def __init__(self, reason = "no data detected to migrate"):
        self.msg = reason

    def __str__(self):
        return repr(self.msg)
        
class ColumnTransformationController():
    def __init__(self):
        self.columns = []
        self.msg = f"can not create the columns: "

    def add_table(self, table_name):
        self.columns.append(table_name)
        
    def get_msg(self):
        return  self.msg + ','.join(self.columns)
        
    def is_empty(self):
        return len(self.columns) == 0

def split_parameters(query_string):
    aux = []
    params = []
    last_param = 0
    query_string += ','
    for index in range(len(query_string)):
        a = query_string[index]
        if a == ',' and len(aux) == 0:
            params.append(query_string[last_param:index])
            last_param = index + 1
        elif a == '(':
            aux.append(index)
        elif a == ')':
            aux.pop()
    return(params)
    
def split_function(query_string):
    aux = []
    for index in range(len(query_string)):
        a = query_string[index]
        if  a == '(':
            aux.append(index)
        elif a == ')':
            all_parameters.append(query_string[aux[-1]+1:index])
            if len(aux) <= 1:
                start_index =  0
            else:
                last_function_index = aux[-2]
                last_comma_separator =  query_string[:aux[-1]].rfind(',')
                if last_comma_separator != -1:
                    start_index = last_comma_separator + 1
                else: 
                    start_index =  last_function_index + 1
            functions.append(query_string[start_index:aux[-1]])
            aux.pop()

def transform_df(raw_df, function_name, parameters, column_name, data_type):
    function_name = function_name.strip()
    if '$sub_column' in column_name:
        column_name = f"{function_name}({parameters})"

    # logger.info(f"adding column: {column_name}")
    # logger.info(f"function name : {function_name}")
    # logger.info(f"parameters : {parameters}")
    
    list_params = split_parameters(parameters)
    # logger.info(f"lista de parametros: {list_params}")
    if function_name == 'fn_transform_Concatenate':
        columns_to_concatenate = [col_name.strip() for col_name in list_params]
        return raw_df.withColumn(column_name, concat_ws("|", *[trim(col(col_name)) for col_name in columns_to_concatenate]).cast(data_type))
    
    elif function_name == 'fn_transform_Concatenate_ws':
        columns_to_concatenate = [col_name.strip() for col_name in list_params[:-1]]
        return raw_df.withColumn(column_name, concat_ws(list_params[-1], *[trim(col(col_name)) for col_name in columns_to_concatenate]).cast(data_type))
    
    elif function_name == 'fn_transform_ByteMagic':
        origin_column = list_params[0]
        default = list_params[1]
        columns_to_concatenate = [col_name.strip() for col_name in list_params[:-1]]
        if '$' in default:
            return raw_df.withColumn(column_name, when(col(origin_column) == 'T', 'T').when(col(origin_column) == 'F', 'F').otherwise(lit(default.replace('$',''))).cast(data_type))
        else:
            return raw_df.withColumn(column_name, when(col(origin_column) == 'T', 'T').when(col(origin_column) == 'F', 'F').otherwise(col(default)).cast(data_type))
        
    elif function_name == 'fn_transform_Case':
        origin_column = list_params[0]
        rules = list_params[1:]
        for ele_case in rules:
            value_case = ele_case.split('->')[0]
            label_case = ele_case.split('->')[1]
            values_to_change = value_case.split('|')
            raw_df = raw_df.withColumn(column_name, when(col(origin_column).isin(values_to_change), label_case).cast(data_type))
        return raw_df
        
    elif function_name == 'fn_transform_Case_with_default':
        origin_column = list_params[0]
        rules = list_params[1:-1]
        default = list_params[-1]
        total_changes = []
        
        if '$' in default:
            raw_df = raw_df.withColumn(column_name, lit(default.replace('$','')).cast(data_type)) 
        else:
            raw_df = raw_df.withColumn(column_name, col(default).cast(data_type).cast(data_type))

        if '&' in origin_column:
            conditions = origin_column.split('&')
            condition_expr = None
    
            for ele_case in rules:
                value_case, label_case = ele_case.split('->')
                values_to_change = value_case.split('|')
        
                sub_condition_expr = None
                for value in values_to_change:
                    value_separated = value.split('&')
                    sub_condition = None
                    for i, col_name in enumerate(conditions):
                        if sub_condition is None:
                            sub_condition = (col(col_name) == lit(value_separated[i]))
                        else:
                            sub_condition &= (col(col_name) == lit(value_separated[i]))
            
                    if sub_condition_expr is None:
                        sub_condition_expr = sub_condition
                    else:
                        sub_condition_expr |= sub_condition
        
                if condition_expr is None:
                    condition_expr = sub_condition_expr
                else:
                    condition_expr |= sub_condition_expr
                raw_df = raw_df.withColumn(column_name, when(condition_expr, label_case).otherwise(col(column_name)))
               
        else:
             
            for ele_case in rules:
                value_case = ele_case.split('->')[0]
                label_case = ele_case.split('->')[1]
                values_to_change = value_case.split('|')
                raw_df = raw_df.withColumn(column_name, when(col(origin_column).isin(values_to_change), label_case).otherwise(col(column_name)))
                
        return raw_df
      
    elif function_name == 'fn_transform_Datetime':
        list_params = parameters.split(',') 
        origin_column = list_params[0]
        if list_params[0] == '':
            raw_df = raw_df.withColumn(column_name, from_utc_timestamp(current_timestamp(), "America/Lima").cast(data_type))
        else:
            raw_df = raw_df.withColumn(column_name, to_timestamp(origin_column).cast(data_type))
        return raw_df  
     
    # pending review 
    elif function_name == 'fn_transform_ClearDouble':
        columns_to_concatenate = [col_name.strip() for col_name in list_params[:-1]]
        return raw_df.withColumn(column_name, concat_ws(list_params[-1], *[col(col_name) for col_name in columns_to_concatenate]).cast(data_type))
        
    elif function_name == 'fn_transform_ClearString':
      if len(list_params) > 1:
        origin_column = list_params[0]
        default = list_params[1]
        if "$" in default:
            default = lit(default.replace('$', ''))
        else:
            default = col(default)
      
        return raw_df.withColumn(
            column_name,
            when(col(origin_column).isNotNull(), trim(col(origin_column)))
            .otherwise(default)
            .cast(data_type)
        )
      else:
        origin_column = list_params[0]
        return raw_df.withColumn(column_name, trim(col(origin_column)).cast(data_type))

    elif function_name == 'fn_transform_Date_to_String':
        return raw_df.withColumn(column_name, date_format(col(list_params[0]), list_params[1]).cast(data_type))
    
    elif function_name == 'fn_transform_DateMagic':
        base_date = "1900-01-01"
        value_default = list_params[-1]
        date_format_parameter = list_params[1]
        origin_column = list_params[0]
        date_pattern = r'^([7-9]\d{5}|[1-2]\d{6}|3[0-5]\d{5})$'
        return raw_df.withColumn(
            column_name,
            when(
                regexp_extract(col(origin_column).cast(StringType()), date_pattern, 1) != "",
                to_date(date_add(to_date(lit(base_date)), col(origin_column).cast(IntegerType()) - lit(693596)), date_format_parameter)
            ).otherwise(
                to_date(lit(value_default), date_format_parameter)
            ).cast(data_type)
        )
        
    elif function_name == 'fn_transform_DatetimeMagic':
        base_date = "1900-01-01"
        value_default = list_params[-1]
        datetime_format = list_params[2]
        origin_column_date = list_params[0]
        origin_column_time = list_params[1]
        date_pattern = r'^([7-9]\d{5}|[1-2]\d{6}|3[0-5]\d{5})$'
        time_pattern = r'^([01][0-9]|2[0-3])([0-5][0-9])([0-5][0-9])$'
        
        return raw_df.withColumn(
            column_name,
            when(
                regexp_extract(col(origin_column_date).cast(StringType()), date_pattern, 1) != "",
                when(
                    regexp_extract(col(origin_column_time).cast(StringType()), time_pattern, 1) != "",
                    to_timestamp(
                        concat_ws(" ", 
                            to_date(date_add(to_date(lit(base_date)), col(origin_column_date).cast(IntegerType()) - lit(693596))),
                            concat_ws(":", col(origin_column_time).substr(1, 2), col(origin_column_time).substr(3, 2), col(origin_column_time).substr(5, 2))
                        ),  datetime_format
                    )
                )
                .otherwise(
                    to_timestamp(date_add(to_date(lit(base_date)), col(origin_column_date).cast(IntegerType()) - lit(693596)), datetime_format[:8])
                )
            )
            .otherwise(
                to_timestamp(lit(value_default), datetime_format[:8])
            ).cast(data_type)
        )

    
    elif function_name == 'fn_transform_PeriodMagic':
        period_value = list_params[0]
        ejercicio_value = list_params[1]
        return raw_df.withColumn(
            column_name,
            when(
                (col(period_value).isNull()),
                '190001'
            ).otherwise(
                concat(period_value, lpad(ejercicio_value, 2, '0'))
            ).cast(data_type)
        )

    else:
        return raw_df

def send_error_message(topic_arn, table_name, error):
    client = boto3.client("sns")
    if "no data detected to migrate" in error:
        message = f"RAW WARNING in table: {table_name} \n{error}"
    else:
        message = f"Failed table: {table_name} \nStep: stage job \nLog ERROR \n{error}"
    response = client.publish(
        TopicArn=topic_arn,
        Message=message
    )

def add_log_to_dynamodb(table_name, record):
    dynamodb = boto3.resource('dynamodb')
    dynamo_table = dynamodb.Table(table_name)
    dynamo_table.put_item(Item=record)

def condition_generator(id_columns):
    id_columns_list = id_columns.split(",")
    string = ""
    for id_column in id_columns_list:
        string = "old." + id_column + "=new." + id_column + " AND " + string
    return string[:-4]

def create_empty_dataframe_with_schema(columns_metadata, spark):
    """
    Crea un DataFrame vacío con el esquema basado en los metadatos de las columnas.
    Las columnas están ordenadas por COLUMN_ID.
    """
    # Mapeo de tipos de datos
    type_mapping = {
        'string': StringType(),
        'int': IntegerType(),
        'integer': IntegerType(),
        'double': DoubleType(),
        'float': DoubleType(),
        'boolean': BooleanType(),
        'timestamp': TimestampType(),
        'date': DateType()
    }

    fields = []
    columns_order = []
    # logger.info(f"Creating empty DataFrame with columns metadata: {columns_metadata}")
    for column in columns_metadata['Items']:
        column_name = get_column_value(column, 'COLUMN_NAME')
        data_type_str = get_column_value(column, 'NEW_DATA_TYPE').lower()
        data_type = type_mapping.get(data_type_str, StringType())

        fields.append(StructField(column_name, data_type, True))
        columns_order.append({
            'name': column_name,
            'column_id': int(column['COLUMN_ID']['N'])
        })

    # Ordenar columnas por COLUMN_ID
    columns_order.sort(key=lambda x: x['column_id'])
    ordered_column_names = [col['name'] for col in columns_order]

    # Reordenar campos según el orden
    ordered_fields = [next(field for field in fields if field.name == col_name)
                      for col_name in ordered_column_names]

    schema = StructType(ordered_fields)
    # logger.info(f"Creating empty DataFrame with schema: {schema}")
    empty_df = spark.createDataFrame([], schema)

    return empty_df

is_empty_source = False
row_count = 0
try:
    #DAYS_LIMA = '26'
    # Get clean table name (remove alias after space) for S3 raw path  
    clean_table_name = get_clean_table_name(table_data, table_name)

    day_route = f"{args['TEAM']}/{args['DATA_SOURCE']}/{args['ENDPOINT_NAME']}/{clean_table_name}/year={YEARS_LIMA}/month={MONTHS_LIMA}/day={DAYS_LIMA}/"
    s3_raw_path = f"s3://{s3_source}/{day_route}"
    s3_stage_path = f"s3://{s3_target}/{args['TEAM']}/{args['DATA_SOURCE']}/{args['ENDPOINT_NAME']}/{table_name}/"
    # Try to read as Parquet first
    try:
        raw_df = spark.read.format("parquet").load(s3_raw_path)      
        row_count = raw_df.count()
        logger.info("Successfully read data as Parquet format")
    except Exception as parquet_error:
        logger.warning(f"Failed to read as Parquet: {parquet_error}")
        raise NoDataToMigrateException()  
    else:
        if row_count == 0:
            logger.warning("DataFrame is empty (only headers or completely empty)")
            # Crear DataFrame vacío con esquema correcto
            raw_df = create_empty_dataframe_with_schema(columns_metadata, spark)
            partition_columns_array = []
            if not DeltaTable.isDeltaTable(spark, s3_stage_path):
                for column in columns_metadata['Items']:
                    logger.info(f"column : {column['COLUMN_NAME']['S']}")
                    if column.get('IS_PARTITION', {}).get('BOOL', False):
                        column_name = get_column_value(column, 'COLUMN_NAME')
                        partition_columns_array.append(column_name)
                if len(partition_columns_array) > 0:
                    logger.info(f"Writing empty table with partitionBy: {partition_columns_array}")
                    raw_df.write.partitionBy(*partition_columns_array).format("delta").mode("overwrite").save(s3_stage_path)
                else:
                    logger.info("Writing empty table without partitioning")
                    raw_df.write.format("delta").mode("overwrite").save(s3_stage_path)
                is_empty_source = True
                logger.info("Created empty DataFrame with schema and saved to stage path")
            else:
                logger.info("Delta table already exists, skipping creation of empty DataFrame")
                is_empty_source = True
        else:                    
            columns_controller = ColumnTransformationController()
            partition_columns_array = []
            id_columns = ''
            columns_order = []
            order_by_columns = []
            filter_date_columns = ''
            for column in columns_metadata['Items']:
                try:
                    column_name = get_column_value(column, 'COLUMN_NAME')
                    logger.info(f"Processing column: {column_name}, IS_PARTITION: {column.get('IS_PARTITION', {})}")
                    if column.get('IS_PARTITION', {}).get('BOOL', False):
                        partition_columns_array.append(column_name)
                        logger.info(f"Added partition column: {column_name}")
                    if column.get('IS_ORDER_BY', {}).get('BOOL', False):
                        order_by_columns.append(column_name)
                    if column.get('IS_ID', {}).get('BOOL', False):
                        id_columns += column_name + ','
                    if column.get('IS_FILTER_DATE', {}).get('BOOL', False):
                        filter_date_columns += column_name + ','
                
                    functions = []
                    all_parameters = [] 
                    query_string = get_column_value(column, 'TRANSFORMATION')
                    
                    if query_string.count("(") == query_string.count(")"):
                        query = split_function(query_string)
                        # logger.info(f"function : {functions}")
                        # logger.info(f"parameters : {all_parameters}")
        
                    else:
                        raise Exception("query transformation error with column ", get_column_value(column, 'COLUMN_NAME'))
                        
                    if len(functions) == 0:
                        column_name = get_column_value(column, 'COLUMN_NAME').strip()
                        transformation = get_column_value(column, 'TRANSFORMATION').strip()
                        data_type = get_column_value(column, 'NEW_DATA_TYPE')
                        raw_df = raw_df.withColumn(column_name, col(transformation).cast(data_type))
                    else:
                        data_type = get_column_value(column, 'NEW_DATA_TYPE')
                        for i in range(len(functions)-1):
                            raw_df = transform_df(raw_df, functions[i], all_parameters[i], "$sub_column", data_type)
                            raw_df.show()
                        column_name = get_column_value(column, 'COLUMN_NAME').strip()
                        raw_df = transform_df(raw_df, functions[-1], all_parameters[-1], column_name, data_type)
                    
                    column_name = get_column_value(column, 'COLUMN_NAME')
                    column_id = get_column_value(column, 'COLUMN_ID')
                    columns_order.append({'name': column_name, 'column_id': int(column_id)})
                    #raw_df.show()
                except Exception as e:
                    column_name = get_column_value(column, 'COLUMN_NAME')
                    columns_controller.add_table(column_name)
                    log = {
                        'PROCESS_ID': f"DLB_{table_name.split('_')[0]}_{table_name}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
                        'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
                        'PROJECT_NAME': project_name,
                        'FLOW_NAME': 'light_transform',
                        'TASK_NAME': 'light_transform_table',
                        'TASK_STATUS': 'error',
                        'MESSAGE': f"{e}",
                        'PROCESS_TYPE': 'D' if table_data.get('LOAD_TYPE', 'full').strip() in ['incremental'] else 'F',
                        'CONTEXT': f"{{server='[{endpoint_data['SRC_DB_NAME']},{endpoint_data['SRC_SERVER_NAME']}]', user='{endpoint_data['SRC_DB_USERNAME']}', table='{table_name}'}}"
                    }
                    add_log_to_dynamodb(dynamo_logs_table, log) 
                    logger.error(e)

            #Drop duplicates according to id columns and filter date columns (get the last date)
            if filter_date_columns != '':
                dd_filter_date_columns = filter_date_columns[:-1]
                dd_filter_date_columns = dd_filter_date_columns.split(',')
                dd_id_columns = id_columns.split(',')
                dd_id_columns = dd_id_columns[:-1]
                # logger.info(f"drop duplicates according to id columns: {dd_id_columns}")
                # logger.info(f"drop duplicates according to filter date columns: {dd_filter_date_columns}")
                #descending order by
                order_by_columns = [col(column).desc() for column in dd_filter_date_columns]
                # logger.info(f"order by columns: {order_by_columns}")
                window = Window.partitionBy(dd_id_columns).orderBy(order_by_columns)
                raw_df = raw_df.withColumn("row_number", row_number().over(window))
                raw_df = raw_df.filter(col("row_number") == 1).drop("row_number")

            new_colums = [item['name'] for item in sorted(columns_order, key=lambda x: x['column_id'])]
            # logger.info(new_colums)
            # logger.info(f"partition columns: {partition_columns_array}")
            # logger.info(f"new columns to add: {new_colums}")
            # logger.info(f"oreder by columns: {order_by_columns}")
            logger.info(f"Final partition columns array: {partition_columns_array}")
            raw_df = raw_df.select(*new_colums).orderBy(order_by_columns)
            
            # logger.info(f"total rows: {row_count}")
            max_tries = 3
            actual_try = 0
            
            if DeltaTable.isDeltaTable(spark, s3_stage_path) and row_count > 0:
                while actual_try != max_tries:
                    try:
                        if table_data.get('LOAD_TYPE', 'full').strip() not in ['incremental', 'between-date']:
                            if len(partition_columns_array) > 0:
                                logger.info(f"Writing with partitionBy: {partition_columns_array}")
                                raw_df.write.partitionBy(*partition_columns_array).format("delta").mode("overwrite").save(s3_stage_path)
                            else:
                                logger.info("Writing without partitioning")
                                raw_df.write.format("delta").mode("overwrite").save(s3_stage_path)
                        else:
                            condition = condition_generator(id_columns[:-1])
                            ids_to_drop_duplicates = id_columns[:-1].split()
                            # logger.info(condition)
                            raw_df = raw_df.dropDuplicates(ids_to_drop_duplicates)
                            stage_dt = DeltaTable.forPath(spark, s3_stage_path)
                            
                            if table_data.get('SOURCE_TABLE_TYPE','m') == 't':
                                upper_limit = dt.datetime.now(TZ_LIMA)
                                lower_limit = upper_limit - relativedelta(months=(-1*int(table_data.get('DELAY_INCREMENTAL_INI', '-2').strip().replace("'", ""))))
                                stage_dt.delete(col("processperiod") >= int(lower_limit.strftime('%Y%m')))
                            stage_dt.alias("old") \
                                .merge(raw_df.alias("new"), condition) \
                                .whenMatchedUpdateAll().whenNotMatchedInsertAll() \
                                .execute()
                        break
                    except Exception as e:
                        actual_try += 1
                        time.sleep(actual_try * 60)
                        if actual_try == max_tries:
                            raise Exception(e)
            else:
                if row_count > 0:
                    # create delta table on stage
                    if len(partition_columns_array) > 0:
                        logger.info(f"Creating new delta table with partitionBy: {partition_columns_array}")
                        raw_df.write.partitionBy(*partition_columns_array).format("delta").mode("overwrite").save(s3_stage_path)
                    else:
                        logger.info("Creating new delta table without partitioning")
                        raw_df.write.format("delta").mode("overwrite").save(s3_stage_path)
                else:
                    # only if theres no data on stage and new dataset is empty 
                    logger.info("Creating empty delta table with repartition")
                    raw_df.repartition(1).write.format("delta").mode("overwrite").save(s3_stage_path)
                    deltaTable = DeltaTable.forPath(spark, s3_stage_path)
                    deltaTable.vacuum(100)
                    deltaTable.generate("symlink_format_manifest")
                    raise NoDataToMigrateException() 
                    
            deltaTable = DeltaTable.forPath(spark, s3_stage_path)
            deltaTable.vacuum(100)
            deltaTable.generate("symlink_format_manifest")
            log = {
                'PROCESS_ID': f"DLB_{table_name.split('_')[0]}_{table_name}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
                'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
                'PROJECT_NAME': project_name,
                'FLOW_NAME': 'light_transform',
                'TASK_NAME': 'light_transform_table',
                'TASK_STATUS': 'satisfactorio',
                'MESSAGE': '',
                'PROCESS_TYPE': 'D' if table_data.get('LOAD_TYPE', 'full').strip() in ['incremental'] else 'F',
                'CONTEXT': f"{{server='[{endpoint_data['SRC_DB_NAME']},{endpoint_data['SRC_SERVER_NAME']}]', user='{endpoint_data['SRC_DB_USERNAME']}', table='{table_name}'}}"
            }
            add_log_to_dynamodb(dynamo_logs_table, log)  
            
            if columns_controller.is_empty():
                logger.info(f"All columns transformed successfully - no transformation errors")
                add_log_to_dynamodb(dynamo_logs_table, log) 
            else:
                logger.info(f"Column transformation errors detected: {columns_controller.get_msg()}")
                log = {
                    'PROCESS_ID': f"DLB_{table_name.split('_')[0]}_{table_name}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
                    'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
                    'PROJECT_NAME': project_name,
                    'FLOW_NAME': 'light_transform',
                    'TASK_NAME': 'light_transform_table',
                    'TASK_STATUS': 'WARNING',
                    'MESSAGE': f"STAGE: {columns_controller.get_msg()}",
                    'PROCESS_TYPE': 'D' if table_data.get('LOAD_TYPE', 'full').strip() in ['incremental'] else 'F',
                    'CONTEXT': f"{{server='[{endpoint_data['SRC_DB_NAME']},{endpoint_data['SRC_SERVER_NAME']}]', user='{endpoint_data['SRC_DB_USERNAME']}', table='{table_name}'}}"
                }
                add_log_to_dynamodb(dynamo_logs_table, log) 
            
except NoDataToMigrateException as e:
    log = {
        'PROCESS_ID': f"DLB_{table_name.split('_')[0]}_{table_name}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
        'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
        'PROJECT_NAME': project_name,
        'FLOW_NAME': 'light_transform',
        'TASK_NAME': 'light_transform_table',
        'TASK_STATUS': 'error',
        'MESSAGE': f"No data detected to migrate. Details are: {e}",
        'PROCESS_TYPE': 'D' if table_data.get('LOAD_TYPE', 'full').strip() in ['incremental'] else 'F',
        'CONTEXT': f"{{server='[{endpoint_data['SRC_DB_NAME']},{endpoint_data['SRC_SERVER_NAME']}]', user='{endpoint_data['SRC_DB_USERNAME']}', table='{table_name}'}}"
    }
    add_log_to_dynamodb(dynamo_logs_table, log)     
    send_error_message(args['ARN_TOPIC_FAILED'], table_data.get('STAGE_TABLE_NAME', table_name), str(e))
    logger.error(f"No data detected to migrate. Details are: {e}")

except Exception as e:
    log = {
        'PROCESS_ID': f"DLB_{table_name.split('_')[0]}_{table_name}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
        'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
        'PROJECT_NAME': project_name,
        'FLOW_NAME': 'light_transform',
        'TASK_NAME': 'light_transform_table',
        'TASK_STATUS': 'error',
        'MESSAGE': f"{e}",
        'PROCESS_TYPE': 'D' if table_data.get('LOAD_TYPE', 'full').strip() in ['incremental'] else 'F',
        'CONTEXT': f"{{server='[{endpoint_data['SRC_DB_NAME']},{endpoint_data['SRC_SERVER_NAME']}]', user='{endpoint_data['SRC_DB_USERNAME']}', table='{table_name}'}}"
    }
    add_log_to_dynamodb(dynamo_logs_table, log) 
    send_error_message(args['ARN_TOPIC_FAILED'], table_data.get('STAGE_TABLE_NAME', table_name), str(e))
    logger.error(f"Error while importing data. Details are: {e}")
