"""
Lambda function to invoke the domain Step Function after all instance extractions are complete.

This function receives:
- process_id: The process ID to pass to the Step Function
- instance: The instance name being processed

Returns:
- Execution details and result from the Step Function
"""

import boto3
import json
import logging
import time
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Main Lambda handler function to invoke domain step function
    """
    try:
        # Extract inputs - now using the original instance step function input parameters
        process_id = event.get('process_id')
        endpoint_name = event.get('endpoint_name')  # Single endpoint from original input
        instance = event.get('instance')
        scheduled_execution = event.get('scheduled_execution', True)
        endpoint_names = event.get('endpoint_names', [])  # Array of all endpoints processed
        execution_results = event.get('execution_results', {})
        
        logger.info(f"Processing instance: {instance}, process_id: {process_id}, original_endpoint: {endpoint_name}, all_endpoints: {endpoint_names}")
        
        # Validate required inputs
        if not process_id:
            raise ValueError("process_id is required")
        if not instance:
            raise ValueError("instance is required")
        
        # Get domain step function ARN from environment variable
        domain_step_function_arn = os.environ.get('DOMAIN_STEP_FUNCTION_ARN')
        if not domain_step_function_arn:
            # Fallback to constructing the ARN dynamically if not provided as environment variable
            # Note: AWS_REGION is automatically available in Lambda, but we use custom env vars to avoid conflicts
            aws_region = os.environ.get('DATALAKE_AWS_REGION') or os.environ.get('AWS_REGION', 'us-east-2')
            aws_account_id = os.environ.get('DATALAKE_AWS_ACCOUNT_ID', '832257724409')
            environment = os.environ.get('DATALAKE_ENVIRONMENT', 'dev')
            
            # Construct the domain step function ARN based on naming convention
            domain_step_function_arn = f"arn:aws:states:{aws_region}:{aws_account_id}:stateMachine:aje-{environment.lower()}-datalake-workflow_domain_bigmagic_group_{process_id}-sf"
            logger.info(f"Constructed domain step function ARN: {domain_step_function_arn}")
        else:
            logger.info(f"Using domain step function ARN from environment: {domain_step_function_arn}")
        
        # Create Step Functions client
        client = boto3.client('stepfunctions')
        
        # If we have a specific endpoint_name from the original input, use that
        # Otherwise, process all endpoint_names that were processed
        endpoints_to_process = [endpoint_name] if endpoint_name else endpoint_names
        execution_arns = []
        
        for endpoint_to_process in endpoints_to_process:
            # Prepare input for the domain Step Function matching the instance step function input structure
            step_function_input = {
                "process_id": process_id,
                "endpoint_name": endpoint_to_process,
                "instance": instance,
                "scheduled_execution": scheduled_execution
            }
            
            logger.info(f"Starting execution of Domain Step Function for endpoint {endpoint_to_process}: {domain_step_function_arn}")
            logger.info(f"Step Function input: {json.dumps(step_function_input)}")
            
            # Start execution for this endpoint
            response = client.start_execution(
                stateMachineArn=domain_step_function_arn,
                input=json.dumps(step_function_input),
                name=f"domain-{instance}-{endpoint_to_process}-{process_id}-{int(time.time())}"
            )
            
            execution_arn = response['executionArn']
            execution_arns.append({
                'endpoint_name': endpoint_to_process,
                'execution_arn': execution_arn
            })
            logger.info(f"Domain Step Function execution started for {endpoint_to_process}: {execution_arn}")
        
        return {
            'statusCode': 200,
            'instance': instance,
            'process_id': process_id,
            'original_endpoint': endpoint_name,
            'processed_endpoints': endpoints_to_process,
            'executions': execution_arns,
            'total_executions': len(execution_arns),
            'status': 'RUNNING',
            'message': f'Domain Step Function executions started successfully for {len(execution_arns)} endpoints'
        }
        
    except client.exceptions.StateMachineDoesNotExist as e:
        logger.error(f"Domain Step Function does not exist: {domain_step_function_arn}")
        logger.error(f"Error details: {str(e)}")
        return {
            'statusCode': 404,
            'error': 'StateMachineDoesNotExist',
            'message': f"Domain Step Function not found",
            'domain_step_function_arn': domain_step_function_arn,
            'instance': instance,
            'process_id': process_id,
            'original_endpoint': endpoint_name,
            'processed_endpoints': endpoints_to_process
        }
        
    except client.exceptions.ExecutionLimitExceeded as e:
        logger.error(f"Domain Step Function execution limit exceeded: {str(e)}")
        return {
            'statusCode': 429,
            'error': 'ExecutionLimitExceeded',
            'message': str(e),
            'instance': instance,
            'process_id': process_id,
            'original_endpoint': endpoint_name,
            'processed_endpoints': endpoints_to_process,
            'partial_executions': execution_arns
        }
        
    except client.exceptions.ExecutionDoesNotExist as e:
        logger.error(f"Domain Step Function execution does not exist: {str(e)}")
        return {
            'statusCode': 404,
            'error': 'ExecutionDoesNotExist',
            'message': str(e),
            'instance': instance,
            'process_id': process_id,
            'original_endpoint': endpoint_name,
            'processed_endpoints': endpoints_to_process
        }
        
    except Exception as e:
        logger.error(f"Error invoking Domain Step Function: {str(e)}")
        return {
            'statusCode': 500,
            'error': 'InternalError',
            'message': str(e),
            'instance': instance,
            'process_id': process_id,
            'original_endpoint': endpoint_name,
            'processed_endpoints': endpoints_to_process,
            'partial_executions': execution_arns
        }
