import datetime
import logging
import os
import time
import json
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def find_key(data, key_to_find):
    """
    Busca recursivamente una clave en un diccionario anidado y devuelve la clave y su valor.
    
    Args:
        data: El diccionario en el que buscar
        key_to_find: La clave que se busca
        
    Returns:
        Un diccionario con la clave encontrada y su valor, o None si no se encuentra
    """
    # Caso base: si data es un diccionario
    if isinstance(data, dict):
        # Comprueba si la clave está en este nivel
        if key_to_find in data:
            return {key_to_find: data[key_to_find]}
        
        # Busca en los valores del diccionario
        for key, value in data.items():
            result = find_key(value, key_to_find)
            if result:
                return result
    
    # Si data es una lista, busca en cada elemento
    elif isinstance(data, list):
        for item in data:
            result = find_key(item, key_to_find)
            if result:
                return result
    
    # No se encontró
    return None
    
def send_success_message(arn_topic_success, endpoint_name, process_id):
    client = boto3.client("sns")
    logger.info(f"sending succeded message for {endpoint_name} : {process_id}")
    response = client.publish(
        TopicArn=arn_topic_success,
        Message=f"successfully load tables from process : {process_id} Source : {endpoint_name}"
    )

def lambda_handler(event, context):
    try:
        logger.info(event)
        client = boto3.resource('dynamodb')
        dynamo_config_table = os.getenv('DYNAMO_CONFIG_TABLE')
        arn_topic_success = os.getenv("ARN_TOPIC_SUCCESS")
        db_table_config = client.Table(dynamo_config_table)
        
        key = find_key(event, "stage_job_result")
        if key is None:
            logger.error("Key 'stage_job_result' not found in event")
            return {
                'result': "FAILED",
                'endpoint': "",
                'table_names': "",
                'message_error': "Key 'stage_job_result' not found in event"
            }

        table = key['stage_job_result']['Arguments']['--TABLE_NAME']
          
        table_names = ""
        table_data = db_table_config.get_item(Key={'TARGET_TABLE_NAME': table})['Item']
        endpoint = table_data['ENDPOINT']
        process_id = table_data['PROCESS_ID']
        
        try:
            raw_failed_tables = db_table_config.scan(
                FilterExpression=f"ENDPOINT = :val1 AND ACTIVE_FLAG = :val2 AND STATUS_RAW = :val3 ",
                ExpressionAttributeValues={
                    ':val1': endpoint,
                    ':val2': 'Y',
                    ':val3': 'FAILED'
                }
            )
            logger.info(f"failed tables in raw: {raw_failed_tables}")

            stage_failed_tables = db_table_config.scan(
                FilterExpression=f"ENDPOINT = :val1 AND ACTIVE_FLAG = :val2 AND STATUS_STAGE = :val3 ",
                ExpressionAttributeValues={
                    ':val1': endpoint,
                    ':val2': 'Y',
                    ':val3': 'FAILED'
                }
            )
            logger.info(f"failed tables in raw: {stage_failed_tables}")
            #if (not 'Items' in raw_failed_tables.keys() or raw_failed_tables['Items'] == []) and (not 'Items' in stage_failed_tables.keys() or raw_failed_tables['Items'] == []):
            #    send_success_message(arn_topic_success, endpoint, process_id)

        except Exception as e:
            logger.error(str(e))

        return {
            'result': "SUCCESS",
            'endpoint': endpoint,
            'table_names': table_names,
            'process_id': process_id
        }

    except Exception as e:
        logger.error("Exception: {}".format(e))
        return {
            'result': "FAILED",
            'endpoint': "",
            'table_names': "",
            'message_error': str(e)
        }
    