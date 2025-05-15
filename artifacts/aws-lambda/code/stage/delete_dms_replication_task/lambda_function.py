import json
import logging
import os

import boto3
import botocore

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def delete_task(dms_task_arn):
    client = boto3.client('dms')
    try:
        response = client.delete_replication_task(ReplicationTaskArn=dms_task_arn)
        return "SUCCEDED"
    except Exception as e:
        logger.error("Could not start dms task: %s" % e)
        return "FAILED"


def lambda_handler(event, context):
    result = 'SUCCEDED'
    delete_failed_tasks = []
    try:
        dynamo_db_key = event['dynamodb_key']
        dms_tasks_arn = event['task_arn']
        for task_arn in dms_tasks_arn:
            delete_task_response = delete_task(task_arn)
            if delete_task_response == "FAILED":
                result = "FAILED"
                delete_failed_tasks.append(task_arn)
        return {"result": result,
                "dynamodb_key": dynamo_db_key,
                "delete_failed_tasks": delete_failed_tasks,
                'replication_instance_arn': event['replication_instance_arn']}

    except botocore.exceptions.ClientError as e:
        logger.error("Could not start dms task: %s" % e)
        return {"result": result}
