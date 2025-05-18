import logging
import os
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        logger.info("event : " + str(event))
        client = boto3.resource('dynamodb')
        dynamo_config_table = os.getenv('DYNAMO_CONFIG_TABLE')
        db_table_config = client.Table(dynamo_config_table)
        result = []
        
        for table in event['dynamodb_key']:
            table_data = db_table_config.get_item(Key={'TARGET_TABLE_NAME': table})
            if 'Item' in table_data:
                result.append({"table": table})

        return {
            'result': "SUCCEEDED",
            'dynamodb_key': result,
            'process': event['process'],
            'execute_raw': event['execute_raw']            
        }
    except Exception as e:
        logger.info("exception : " + str(e))
        return {
            'result': "FAILED",
            'dynamodb_key': []
        }