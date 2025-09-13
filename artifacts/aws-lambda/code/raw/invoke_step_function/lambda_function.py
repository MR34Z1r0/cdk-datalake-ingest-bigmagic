"""
Lambda function to prepare input parameters for group step function invocation.

This lambda is called by the instance step function to prepare input data
for the specific group step function that will be invoked synchronously.

This function receives:
- process_id: The process ID to pass to the Step Function
- endpoint_name: The endpoint name being processed
- instance: The instance name

Returns:
- Prepared input parameters for the group step function
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Prepare input parameters for group step function invocation.
    This lambda is called by the instance step function to prepare input data
    for the specific group step function that will be invoked synchronously.
    
    Args:
        event: Input event containing process_id, endpoint_name, and instance
        
    Returns:
        Dict containing prepared input parameters for the group step function
    """
    try:
        logger.info(f"Processing input preparation request: {json.dumps(event, indent=2)}")
        
        # Extract required parameters
        process_id = event.get('process_id')
        endpoint_name = event.get('endpoint_name')
        instance = event.get('instance')
        
        # Validate required parameters
        if not process_id or not endpoint_name or not instance:
            error_msg = f"Missing required parameters. process_id: {process_id}, endpoint_name: {endpoint_name}, instance: {instance}"
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing required parameters',
                    'message': error_msg,
                    'timestamp': datetime.now().isoformat()
                })
            }
        
        # Extract optional parameters
        run_extract = event.get('run_extract', True)
        scheduled_execution= event.get('scheduled_execution', True)
        additional_params = event.get('additional_params', {})
        
        # Prepare the input for the group step function
        prepared_input = {
            'endpoint_name': endpoint_name,
            'process_id': process_id,
            'instance': instance,
            'scheduled_execution': scheduled_execution,
            'run_extract': run_extract,
            'execution_timestamp': datetime.now().isoformat(),
            'source': 'instance_step_function',
            'prepared_by': 'invoke_step_function_lambda'
        }
        
        # Add any additional parameters
        if additional_params:
            prepared_input.update(additional_params)
        
        logger.info(f"Successfully prepared input for group step function: {json.dumps(prepared_input, indent=2)}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Input prepared successfully',
                'prepared_input': prepared_input,
                'timestamp': datetime.now().isoformat()
            }),
            'prepared_input': prepared_input  # Return directly for step function use
        }
        
    except Exception as e:
        error_msg = f"Error preparing input for group step function: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Input preparation failed',
                'message': error_msg,
                'timestamp': datetime.now().isoformat()
            })
        }
