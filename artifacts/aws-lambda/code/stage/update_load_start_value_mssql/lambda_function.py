import base64
import json
import logging
import os
import time
from datetime import datetime, timedelta

import boto3
import pyodbc
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_table = os.getenv('DYNAMO_DB_TABLE').strip()
dynamodb_credentials = os.getenv('DYNAMO_CREDENTIALS_TABLE').strip()
secrets_name = os.getenv('SECRETS_NAME').strip()


def send_error_message(topic_arn, table_name, error):
    client = boto3.client("sns")
    response = client.publish(
        TopicArn=topic_arn,
        Message=f"Failed table: {table_name}\nStep : update value step  \nLog ERROR: {error}"
    )


def update_attribute_value_dyndb(row_key_field_name, row_key, attribute_name, attribute_value, table_name):
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


def is_valid_date(date_to_validate):
    desired_format = '%Y-%m-%d %H:%M:%S'
    date_to_validate = date_to_validate[:19]
    date_in_date_format = datetime.strptime(date_to_validate, desired_format)
    today = datetime.now().date()
    if date_in_date_format.date() > today:
        yesterday = today - timedelta(days=1)
        yesterday = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)
        return yesterday.strftime(desired_format)
    else:
        return date_to_validate


def update_value_dyndb(row_key_field_name, row_key, attribute_name, attribute_value, table_name):
    dynamodb = boto3.client('dynamodb')
    statement = "UPDATE " + table_name + " SET " + attribute_name + " = '" + attribute_value + "' WHERE " + row_key_field_name + " ='" + row_key + "'"
    response = dynamodb.execute_statement(Statement=statement)


def read_table_metamata(table_name, table_key, key_value):
    logger.info('read_table_meta_data : {} , {}, {}'.format(table_name, table_key, key_value))
    dynamodb = boto3.resource('dynamodb')
    dynamo_table = dynamodb.Table(table_name)
    response = dynamo_table.get_item(Key={table_key: key_value})
    if 'Item' in response:
        item = response['Item']
        return item
    else:
        logger.warning('Metadata Not Found')


def get_secret(secret_name, target_secret, secrets_region):
    region_name = secrets_region[:-1]
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as e:
        logger.error("Exception thrown: %s" % str(e))

    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        secret = json.loads(secret)
        return secret[target_secret]
    return "ERROR"


def lambda_handler(event, context):
    try:
        failed_tables = []
        topic_arn = os.getenv("TOPIC_ARN")
        dynamodb_key = event['dynamodb_key']
        test_value = dynamodb_key[0]
        test_item = read_table_metamata(dynamodb_table, 'TARGET_TABLE_NAME', test_value)
        secrets_region = os.getenv('SECRETS_REGION')
        
        # config for credentials
        credentials = read_table_metamata(dynamodb_credentials, 'ENDPOINT_NAME', test_item['ENDPOINT'])
        user_name = credentials["SRC_DB_USERNAME"]
        password = get_secret(secrets_name, credentials["SRC_DB_SECRET"].strip(), secrets_region)
        src_db_name = credentials["SRC_DB_NAME"]
        src_server_name = credentials["SRC_SERVER_NAME"]
        src_db_port_number = credentials["DB_PORT_NUMBER"]
        connect_url = 'DRIVER={ODBC Driver 17 for SQL server};' + 'SERVER=' + src_server_name + ',' + str(src_db_port_number) + ';' + 'DATABASE=' + src_db_name + ';' + 'UID=' + user_name + ';' + 'PWD=' + password + ';'
        for try_number in range(3):
            try:
                logger.info(f"try {try_number+1}....")
                connection = pyodbc.connect(connect_url)
                cursor = connection.cursor()
                break
            except Exception as e:
                logger.error(str(e))
                if try_number < 2:
                    time.sleep(60 * (try_number + 1))
                elif try_number == 2:
                    raise Exception(str(e))
    except Exception as e:
        event['result'] = 'FAILED'
        event['failed_tables'] = dynamodb_key
        event['error'] = f"Failed endpoint : {test_item['ENDPOINT']} \nStep: update value step \nLog ERROR: {e}"
        logger.error("Exception thrown: %s" % str(e))

        for table in dynamodb_key:
            update_attribute_value_dyndb('TARGET_TABLE_NAME', table, 'STATUS_RAW', 'FAILED', dynamodb_table)
            update_attribute_value_dyndb('TARGET_TABLE_NAME', table, 'STATUS_STAGE', 'FAILED', dynamodb_table)
            update_attribute_value_dyndb('TARGET_TABLE_NAME', table, 'FAIL REASON', str(e), dynamodb_table)
            
        return event

    else:
        aux = dynamodb_key
        for table in dynamodb_key:
            try:
                item = read_table_metamata(dynamodb_table, 'TARGET_TABLE_NAME', table)
                if item['FILTER_OPERATOR'] in ['between-date']:
                    if ',' in item['FILTER_COLUMN']:
                        filter = item['FILTER_COLUMN'].split(',')
                        query = f"SELECT max({filter[0]}) column1, max({filter[1]}) column2 FROM {item['SOURCE_SCHEMA']}.{item['SOURCE_TABLE']}"
                    else:
                        query = f"SELECT max({item['FILTER_COLUMN']}) FROM {item['SOURCE_SCHEMA']}.{item['SOURCE_TABLE']}"
                    cursor.execute(query)
                    result = cursor.fetchone()
                    if len(result) > 1:

                        result1 = result[0]
                        if result1 == None:
                            result1 = datetime(2000, 1, 1)

                        result2 = result[1]
                        if result2 == None:
                            result2 = datetime(2000, 1, 1)

                        if result1 < result2:
                            result = result2
                        else:
                            result = result1
                    else:
                        result = result[0]
                    logger.info("DynamoDb Metadata Record Found! and it's MsSQL Server!")
                    end_value = str(item['END_VALUE']).strip()
                    logger.info("Updating start and end value")
                    start_value = end_value
                    end_value = str(result).strip()
                    end_value = is_valid_date(end_value)
                    update_attribute_value_dyndb('TARGET_TABLE_NAME', table, 'START_VALUE', start_value, dynamodb_table)
                    update_attribute_value_dyndb('TARGET_TABLE_NAME', table, 'END_VALUE', end_value, dynamodb_table)
            except Exception as e:

                send_error_message(topic_arn, table, str(e))
                failed_tables.append(table)
                logger.exception(e)
                aux.remove(table)
                update_attribute_value_dyndb('TARGET_TABLE_NAME', table, 'STATUS_RAW', 'FAILED', dynamodb_table)
                update_attribute_value_dyndb('TARGET_TABLE_NAME', table, 'FAIL REASON', str(e), dynamodb_table)

        event['result'] = 'SUCCEEDED'
        event['dynamodb_key'] = aux
        event['failed_tables'] = failed_tables
        return event
    