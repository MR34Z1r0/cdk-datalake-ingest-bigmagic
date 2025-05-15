import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        client = boto3.resource('dynamodb')
        dynamo_table_name = os.getenv('DYNAMO_DB_TABLE')
        config_table_metadata = client.Table(dynamo_table_name)
        num_tables = int(os.getenv("TABLES_PER_TASK"))
        new_tables = []
        result = []
        array_aux = []
        for table in event['dynamodb_key']:
            table_data = config_table_metadata.get_item(Key={'TARGET_TABLE_NAME': table})['Item']
            if table_data.get('COLUMNS', {'S':''}) != '' or table_data.get('FILTER_COLUMN', {'S':''}) != '' or table_data.get('FILTER_EXP', {'S':''}) != '' or table_data.get('ID_COLUMN', {'S':''}) != '' or table_data.get('JOIN_EXPR', {'S':''}) != '':
                result.append({"type": "needs_glue", "table": table})
            else:
                if len(array_aux) < num_tables:
                    array_aux.append(table)
                else:
                    array_aux.append(table)
                    new_tables.append(array_aux)
                    array_aux = []

        if len(array_aux) != 0:
            new_tables.append(array_aux)

        if len(new_tables) != 0:
            result.append({"type": "dms", "table": new_tables})

        return {
            'result': "SUCCEEDED",
            'dynamodb_key': result,
            'replication_instance_arn': event['replication_instance_arn'],
            'process': event['process']
        }
    except Exception as e:
        logger.info("exception : " + str(e))
        return {
            'result': "FAILED",
            'dynamodb_key': [],
            'replication_instance_arn': event['replication_instance_arn']
        }
    