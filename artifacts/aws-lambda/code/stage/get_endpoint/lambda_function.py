import datetime
import logging
import os
import time
import json
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def send_success_message(topic_arn, endpoint_name, process_id):
    client = boto3.client("sns")
    logger.info(f"sending succeded message for {endpoint_name} : {process_id}")
    response = client.publish(
        TopicArn=topic_arn,
        Message=f"successfully load tables from process : {process_id} Source : {endpoint_name}"
    )


def lambda_handler(event, context):
    try:
        client = boto3.resource('dynamodb')
        dynamo_table_name = os.getenv('DYNAMO_DB_TABLE')
        topic_arn = os.getenv("TOPIC_ARN")
        config_table_metadata = client.Table(dynamo_table_name)
        if event[0].__class__ == list:
            table = event[0][0]['dynamodb_key']
            replication_instance_arn = event[0][0]['replication_instance_arn']

        else:
            if 'Error' in event[0].keys():
                table = json.loads(event[0]['Cause'])['Arguments']['--TABLE_NAME']
                replication_instance_arn = "can't find DMS ARN"
            else:
                table = event[0]['glue_result']['Arguments']['--TABLE_NAME']
                replication_instance_arn = event[0]['replication_instance_arn']

        instance_class = ""
        bd_type = ""
        table_names = ""
        table_data = config_table_metadata.get_item(Key={'TARGET_TABLE_NAME': table})['Item']
        endpoint = table_data['ENDPOINT']
        process_id = table_data['PROCESS_ID']
        
        try:
            raw_failed_tables = config_table_metadata.scan(
                FilterExpression=f"ENDPOINT = :val1 AND ACTIVE_FLAG = :val2 AND STATUS_RAW = :val3 ",
                ExpressionAttributeValues={
                    ':val1': endpoint.strip(),
                    ':val2': 'Y',
                    ':val3': 'FAILED'
                }
            )
            logger.info(f"failed tables in raw: {raw_failed_tables}")

            stage_failed_tables = config_table_metadata.scan(
                FilterExpression=f"ENDPOINT = :val1 AND ACTIVE_FLAG = :val2 AND STATUS_STAGE = :val3 ",
                ExpressionAttributeValues={
                    ':val1': endpoint.strip(),
                    ':val2': 'Y',
                    ':val3': 'FAILED'
                }
            )
            logger.info(f"failed tables in raw: {stage_failed_tables}")
            if (not 'Items' in raw_failed_tables.keys() or raw_failed_tables['Items'] == []) and (not 'Items' in stage_failed_tables.keys() or raw_failed_tables['Items'] == []):
                send_success_message(topic_arn, endpoint, process_id)

        except Exception as e:
            logger.error(str(e))

        return {
            'result': "SUCCESS",
            'replication_instance_arn': replication_instance_arn,
            'instance_class': instance_class,
            'endpoint': endpoint,
            'bd_type': bd_type,
            'table_names': table_names,
            'process_id': process_id
        }

    except Exception as e:
        logger.error("Exception: {}".format(e))
        event[0]['result'] = "FAILED"

        return {
            'result': "FAILED",
            'replication_instance_arn': "",
            'instance_class': "",
            'endpoint': "",
            'bd_type': "",
            'table_names': ""
        }
    