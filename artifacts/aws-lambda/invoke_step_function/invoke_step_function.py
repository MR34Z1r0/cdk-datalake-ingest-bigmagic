"""
Lambda function to dynamically invoke Step Functions for database processing.

This function receives:
- group_step_function_arn: The ARN of the group Step Function to invoke
- process_id: The process ID to pass to the Step Function
- database: The database name being processed
- instance: The instance name

Returns:
- Execution details and result from the Step Function
"""

import boto3
import json
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Main Lambda handler function
    """
    try:
        # Extract inputs
        group_step_function_arn = event.get('group_step_function_arn')
        process_id = event.get('process_id')
        run_extract = event.get('run_extract')
        endpoint_name = event.get('endpoint_name')
        instance = event.get('instance')
        
        logger.info(f"Processing endpoint_name: {endpoint_name}, instance: {instance}, process_id: {process_id}, run_extract: {run_extract}")
        
        # Extract Step Function name from ARN for better error reporting
        step_function_name = group_step_function_arn.split(':')[-1] if group_step_function_arn else "unknown"
        
        # Validate required inputs
        if not group_step_function_arn:
            raise ValueError("group_step_function_arn is required")
        if not process_id:
            raise ValueError("process_id is required")
        
        # Create Step Functions client
        client = boto3.client('stepfunctions')
        
        # Prepare input for the Step Function
        step_function_input = {
            "process_id": process_id,
            "run_extract": run_extract,
            'instance': instance,
            "endpoint_name": endpoint_name
        }
        
        logger.info(f"Starting execution of Step Function: {group_step_function_arn}")
        logger.info(f"Step Function input: {json.dumps(step_function_input)}")
        logger.info(f"Extracted Step Function name: {step_function_name}")
        
        # Start execution with process_id as input
        response = client.start_execution(
            stateMachineArn=group_step_function_arn,
            input=json.dumps(step_function_input)
        )
        
        execution_arn = response['executionArn']
        logger.info(f"Step Function execution started: {execution_arn}")
        
        # Instead of waiting for completion (which can timeout Lambda), return immediately
        # The calling Step Function will handle orchestration and status checking
        logger.info(f"Step Function execution initiated successfully: {execution_arn}")
        
        return {
            'statusCode': 200,
            'endpoint_name': endpoint_name,
            'instance': instance,
            'process_id': process_id,
            'run_extract': run_extract,
            'execution_arn': execution_arn,
            'status': 'RUNNING',
            'message': 'Step Function execution started successfully'
        }
        
    except client.exceptions.StateMachineDoesNotExist as e:
        logger.error(f"Step Function does not exist: {group_step_function_arn}")
        logger.error(f"Expected Step Function name: {step_function_name}")
        logger.error(f"Error details: {str(e)}")
        return {
            'statusCode': 404,
            'error': 'StateMachineDoesNotExist',
            'message': f"Step Function not found: {step_function_name}",
            'step_function_arn': group_step_function_arn,
            'endpoint_name': endpoint_name,
            'run_extract': run_extract,
            'instance': instance,
            'process_id': process_id
        }
        
    except client.exceptions.ExecutionLimitExceeded as e:
        logger.error(f"Step Function execution limit exceeded: {str(e)}")
        return {
            'statusCode': 429,
            'error': 'ExecutionLimitExceeded',
            'message': str(e),
            'endpoint_name': endpoint_name,
            'run_extract': run_extract,
            'instance': instance,
            'process_id': process_id
        }
        
    except client.exceptions.ExecutionDoesNotExist as e:
        logger.error(f"Step Function execution does not exist: {str(e)}")
        return {
            'statusCode': 404,
            'error': 'ExecutionDoesNotExist',
            'message': str(e),
            'endpoint_name': endpoint_name,
            'run_extract': run_extract,
            'instance': instance,
            'process_id': process_id
        }
        
    except Exception as e:
        logger.error(f"Error invoking Step Function: {str(e)}")
        # Safely get step_function_name
        step_function_name = locals().get('step_function_name', 'unknown')
        return {
            'statusCode': 500,
            'error': 'InternalError',
            'message': str(e),
            'step_function_name': step_function_name,
            'endpoint_name': endpoint_name,
            'run_extract': run_extract,
            'instance': instance,
            'process_id': process_id
        }
