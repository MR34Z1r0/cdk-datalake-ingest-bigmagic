import datetime as dt
import logging
import os
import sys
import time

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
from pyspark.sql.functions import col, date_add, to_date, concat_ws, when, regexp_extract, trim, to_timestamp, lit, date_format
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("stage job")
logger.setLevel(os.environ.get("LOGGING", logging.INFO))

TZ_LIMA = pytz.timezone('America/Lima')
YEARS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%Y')
MONTHS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%m')
DAYS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%d')
NOW_LIMA = dt.datetime.now(pytz.utc).astimezone(TZ_LIMA)

# @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'S3_RAW_PREFIX', 'S3_STAGE_PREFIX', 'DYNAMO_CONFIG_TABLE', 'DYNAMO_ENDPOINT_TABLE', 'DYNAMO_LOGS_TABLE', 'TABLE_NAME', 'TOPIC_ARN','DYNAMO_STAGE_COLUMNS', 'PROJECT_NAME', 'TEAM', 'DATA_SOURCE'])

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
dynamo_config_table = args['DYNAMO_CONFIG_TABLE']
dynamo_endpoint_table = args['DYNAMO_ENDPOINT_TABLE']
dynamo_columns_table = args['DYNAMO_STAGE_COLUMNS']
dynamo_logs_table = args['DYNAMO_LOGS_TABLE'].strip()


config_table_metadata = dynamodb.Table(dynamo_config_table)
endpoint_table_metadata = dynamodb.Table(dynamo_endpoint_table)

s3_source = args['S3_RAW_PREFIX']
s3_target = args['S3_STAGE_PREFIX']
table_name = args['TABLE_NAME']

columns_array = dynamodb_client.query(
    TableName=dynamo_columns_table,
    KeyConditionExpression='TARGET_TABLE_NAME  = :partition_key',
    ExpressionAttributeValues={
        ':partition_key': {'S': table_name}
    }
)

table_data = config_table_metadata.get_item(Key={'TARGET_TABLE_NAME': table_name})['Item']
endpoint_data = endpoint_table_metadata.get_item(Key={'ENDPOINT_NAME': table_data['ENDPOINT']})['Item']

class NoDataToMigrateException(Exception):
    def __init__(self):
        self.msg = "no data detected to migrate"

    def __str__(self):
        return repr(self.msg)
        
class column_transformation_controller():
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

    logger.info(f"adding column: {column_name}")
    logger.info(f"function name : {function_name}")
    logger.info(f"parameters : {parameters}")
    
    list_params = split_parameters(parameters)
    logger.info(f"lista de parametros: {list_params}")
    if function_name == 'fn_transform_Concatenate':
        columns_to_concatenate = [col_name.strip() for col_name in list_params]
        return raw_df.withColumn(column_name, concat_ws("|", *[col(col_name) for col_name in columns_to_concatenate]).cast(data_type))
    
    elif function_name == 'fn_transform_Concatenate_ws':
        columns_to_concatenate = [col_name.strip() for col_name in list_params[:-1]]
        return raw_df.withColumn(column_name, concat_ws(list_params[-1], *[col(col_name) for col_name in columns_to_concatenate]).cast(data_type))
    
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

def update_attribute_value_dynamodb(row_key_field_name, row_key, attribute_name, attribute_value, table_name):
    logger.info('update dynamoDb Metadata : {} ,{},{},{},{}'.format(row_key_field_name, row_key, attribute_name, attribute_value, table_name))
    dynamodb = boto3.resource('dynamodb')
    dynamo_table = dynamodb.Table(table_name)
    response = dynamo_table.update_item(
        Key={row_key_field_name: row_key},
        AttributeUpdates={
            attribute_name: {
                'Value': attribute_value,
                'Action': 'PUT'
            }
        }
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

try:
    DAYS_LIMA = '15'
    s3_raw_path = s3_source + args['TEAM'] + "/" + args['DATA_SOURCE'] + "/" + table_data['ENDPOINT'] + "/" + table_data['SOURCE_TABLE'].split()[0] + "/year=" + YEARS_LIMA + "/month=" + MONTHS_LIMA + "/day=" + DAYS_LIMA + "/"
    s3_stage_path = s3_target + args['TEAM'] + "/" + args['DATA_SOURCE'] + "/" + table_data['ENDPOINT'] + "/" + table_data['STAGE_TABLE_NAME'] + "/"
    try:
        raw_df = spark.read.format("csv").option("compression", "gzip").option("header", True).load(s3_raw_path)
        raw_df.show()
    except Exception as e:
        logger.error(e)
        raise NoDataToMigrateException()
    else:
        columns_controller = column_transformation_controller()
        partition_columns_array = []
        id_columns = ''
        columns_order = []
        order_by_columns = []
        filter_date_columns = ''
        for column in columns_array['Items']:
            try:
                logger.info(f"column : {column['COLUMN_NAME']['S']}")
                if column.get('IS_PARTITION', "").__class__ == dict:
                    if column['IS_PARTITION']['BOOL']:
                        partition_columns_array.append(column['COLUMN_NAME']['S'])
                if column.get('IS_ORDER_BY', "").__class__ == dict:
                    if column['IS_ORDER_BY']['BOOL']:
                        order_by_columns.append(column['COLUMN_NAME']['S'])
                if column.get('IS_ID', "").__class__ == dict:
                    if column['IS_ID']['BOOL']:
                        id_columns += column['COLUMN_NAME']['S'] + ','
                if column.get('IS_FILTER_DATE', "").__class__ == dict:
                    if column['IS_FILTER_DATE']['BOOL']:
                        filter_date_columns += column['COLUMN_NAME']['S'] + ','
               
                functions = []
                all_parameters = [] 
                query_string = column['TRANSFORMATION']['S']
                
                if query_string.count("(") == query_string.count(")"):
                    query = split_function(query_string)
                    logger.info(f"function : {functions}")
                    logger.info(f"parameters : {all_parameters}")
    
                else:
                    raise Exception("query transformation error with column ", column['COLUMN_NAME']['S'])
                    
                if len(functions) == 0:
                    raw_df = raw_df.withColumn(column['COLUMN_NAME']['S'].strip(),col(column['TRANSFORMATION']['S'].strip()).cast(column['NEW_DATA_TYPE']['S']))
                else:
                    for i in range(len(functions)-1):
                        raw_df = transform_df(raw_df, functions[i], all_parameters[i], "$sub_column", column['NEW_DATA_TYPE']['S'])
                        raw_df.show()
                    raw_df = transform_df(raw_df, functions[-1], all_parameters[-1], column['COLUMN_NAME']['S'].strip(), column['NEW_DATA_TYPE']['S'])
                columns_order.append({'name' : column['COLUMN_NAME']['S'], 'column_id' : int(column['COLUMN_ID']['N'])})
                raw_df.show()
            except Exception as e:
                columns_controller.add_table(column['COLUMN_NAME']['S'])
                log = {
                    'PROCESS_ID': f"DLB_{table_name.split('_')[0]}_{table_data['SOURCE_TABLE']}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
                    'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
                    'PROJECT_NAME': 'athenea',
                    'FLOW_NAME': 'extract_bigmagic',
                    'TASK_NAME': 'extract_table_raw',
                    'TASK_STATUS': 'error',
                    'MESSAGE': f"{e}",
                    'PROCESS_TYPE': 'D' if table_data['LOAD_TYPE'].strip() in ['incremental'] else 'F',
                    'CONTEXT': f"{{server='[{endpoint_data['ENDPOINT_NAME']},{endpoint_data['SRC_SERVER_NAME']}]', user='{endpoint_data['SRC_DB_USERNAME']}', table='{table_data['SOURCE_TABLE']}'}}"
                }
                add_log_to_dynamodb(dynamo_logs_table, log) 
                logger.error(e)

        #Drop duplicates according to id columns and filter date columns (get the last date)
        if filter_date_columns != '':
            dd_filter_date_columns = filter_date_columns[:-1]
            dd_filter_date_columns = dd_filter_date_columns.split(',')
            dd_id_columns = id_columns.split(',')
            dd_id_columns = dd_id_columns[:-1]
            logger.info(f"drop duplicates according to id columns: {dd_id_columns}")
            logger.info(f"drop duplicates according to filter date columns: {dd_filter_date_columns}")
            #descending order by
            order_by_columns = [col(column).desc() for column in dd_filter_date_columns]
            logger.info(f"order by columns: {order_by_columns}")
            window = Window.partitionBy(dd_id_columns).orderBy(order_by_columns)
            raw_df = raw_df.withColumn("row_number", row_number().over(window))
            raw_df = raw_df.filter(col("row_number") == 1).drop("row_number")


        new_colums = [item['name'] for item in sorted(columns_order, key=lambda x: x['column_id'])]
        logger.info(new_colums)
        logger.info(f"partition columns: {partition_columns_array}")
        logger.info(f"new columns to add: {new_colums}")
        logger.info(f"oreder by columns: {order_by_columns}")
        raw_df = raw_df.select(*new_colums).orderBy(order_by_columns)
        raw_df.show()
        logger.info(f"total rows: {raw_df.count()}")
        max_tries = 3
        actual_try = 0
        
        if DeltaTable.isDeltaTable(spark, s3_stage_path) and raw_df.count() > 0:
            while actual_try != max_tries:
                try:
                    # this is for incremental load
                    if table_data['LOAD_TYPE'].strip() not in ['incremental', 'between-date']:
                        # Assume it is full load, only reason why there is no delta table on stage
                        if len(partition_columns_array) > 0:
                            raw_df.write.partitionBy(*partition_columns_array).format("delta").mode("overwrite").save(s3_stage_path)
                        else:
                            raw_df.write.format("delta").mode("overwrite").save(s3_stage_path)
                        
                    else:
                        condition = condition_generator(id_columns[:-1])
                        ids_to_drop_duplicates = id_columns[:-1].split()
                        logger.info(condition)
                        raw_df = raw_df.dropDuplicates(ids_to_drop_duplicates)
                        stage_dt = DeltaTable.forPath(spark, s3_stage_path)
                        
                        if table_data.get('SOURCE_TABLE_TYPE','m') == 't':
                            upper_limit = dt.datetime.now(TZ_LIMA)
                            lower_limit = upper_limit - relativedelta(months=(-1*int(table_data.get('DELAY_INCREMENTAL_INI', -2))))
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
            if raw_df.count() > 0:
                # create delta table on stage
                if len(partition_columns_array) > 0:
                    raw_df.write.partitionBy(*partition_columns_array).format("delta").mode("overwrite").save(s3_stage_path)
                else:
                    raw_df.write.format("delta").mode("overwrite").save(s3_stage_path)
            else:
                # only if theres no data on stage and new dataset is empty 
                raw_df.repartition(1).write.format("delta").mode("overwrite").save(s3_stage_path)
                deltaTable = DeltaTable.forPath(spark, s3_stage_path)
                deltaTable.vacuum(100)
                deltaTable.generate("symlink_format_manifest")
                raise NoDataToMigrateException() 
                
        deltaTable = DeltaTable.forPath(spark, s3_stage_path)
        deltaTable.vacuum(100)
        deltaTable.generate("symlink_format_manifest")
        log = {
            'PROCESS_ID': f"DLB_{table_name.split('_')[0]}_{table_data['SOURCE_TABLE']}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
            'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
            'PROJECT_NAME': 'athenea',
            'FLOW_NAME': 'extract_bigmagic',
            'TASK_NAME': 'extract_table_raw',
            'TASK_STATUS': 'satisfactorio',
            'MESSAGE': '',
            'PROCESS_TYPE': 'D' if table_data['LOAD_TYPE'].strip() in ['incremental'] else 'F',
            'CONTEXT': f"{{server='[{endpoint_data['ENDPOINT_NAME']},{endpoint_data['SRC_SERVER_NAME']}]', user='{endpoint_data['SRC_DB_USERNAME']}', table='{table_data['SOURCE_TABLE']}'}}"
        }
        add_log_to_dynamodb(dynamo_logs_table, log)  
        
        if columns_controller.is_empty():
            update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_STAGE', 'SUCCEDED', dynamo_config_table)
            update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'FAIL REASON', f"", dynamo_config_table)
        else:
            logger.info(f"there´re problems with columns  : {columns_controller.is_empty()}")
            update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_STAGE', 'WARNING', dynamo_config_table)
            update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'FAIL REASON', f"STAGE: {columns_controller.get_msg()}", dynamo_config_table)
            
except NoDataToMigrateException as e:
    update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_STAGE', 'WARNING', dynamo_config_table)
    update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_RAW', 'WARNING', dynamo_config_table)
    update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'FAIL REASON', f"STAGE: {e}", dynamo_config_table)
    log = {
        'PROCESS_ID': f"DLB_{table_name.split('_')[0]}_{table_data['SOURCE_TABLE']}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
        'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
        'PROJECT_NAME': 'athenea',
        'FLOW_NAME': 'extract_bigmagic',
        'TASK_NAME': 'extract_table_raw',
        'TASK_STATUS': 'error',
        'MESSAGE': f"No data detected to migrate. Details are: {e}",
        'PROCESS_TYPE': 'D' if table_data['LOAD_TYPE'].strip() in ['incremental'] else 'F',
        'CONTEXT': f"{{server='[{endpoint_data['ENDPOINT_NAME']},{endpoint_data['SRC_SERVER_NAME']}]', user='{endpoint_data['SRC_DB_USERNAME']}', table='{table_data['SOURCE_TABLE']}'}}"
    }
    add_log_to_dynamodb(dynamo_logs_table, log)     
    send_error_message(args['TOPIC_ARN'], table_data['TARGET_TABLE_NAME'], str(e))
    logger.error(e)

except Exception as e:
    update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'STATUS_STAGE', 'FAILED', dynamo_config_table)
    update_attribute_value_dynamodb('TARGET_TABLE_NAME', table_data['TARGET_TABLE_NAME'], 'FAIL REASON', f"STAGE: {e}", dynamo_config_table)
    log = {
        'PROCESS_ID': f"DLB_{table_name.split('_')[0]}_{table_data['SOURCE_TABLE']}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}",
        'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
        'PROJECT_NAME': 'athenea',
        'FLOW_NAME': 'extract_bigmagic',
        'TASK_NAME': 'extract_table_raw',
        'TASK_STATUS': 'error',
        'MESSAGE': f"{e}",
        'PROCESS_TYPE': 'D' if table_data['LOAD_TYPE'].strip() in ['incremental'] else 'F',
        'CONTEXT': f"{{server='[{endpoint_data['ENDPOINT_NAME']},{endpoint_data['SRC_SERVER_NAME']}]', user='{endpoint_data['SRC_DB_USERNAME']}', table='{table_data['SOURCE_TABLE']}'}}"
    }
    add_log_to_dynamodb(dynamo_logs_table, log) 
    send_error_message(args['TOPIC_ARN'], table_data['TARGET_TABLE_NAME'], str(e))
    logger.error(e)
    logger.error("Error while importing data")
