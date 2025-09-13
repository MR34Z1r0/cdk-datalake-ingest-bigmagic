import boto3
import json
import logging
import os
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sfn = boto3.client("stepfunctions")

def _build_exec_name(instance: str, endpoint: str, process_id) -> str:
    base = f"legacy-{instance}-{endpoint}-{process_id}"
    # sólo a-zA-Z0-9-_, y recorta para dejar espacio al sufijo
    safe = "".join(c if (c.isalnum() or c in "-_") else "-" for c in base)[:60]
    return f"{safe}-{uuid.uuid4().hex[:8]}"

def lambda_handler(event, context):
    process_id = event.get("process_id")
    instance = event.get("instance")
    endpoint_name = event.get("endpoint_name")
    endpoint_names = event.get("endpoint_names", [])
    scheduled_execution = bool(event.get("scheduled_execution", True))

    # Opcionales para SetDefaults (deben ser objetos si se envían)
    load_redshift = event.get("load_redshift")
    load_ods = event.get("load_ods")

    if not process_id:
        return {"statusCode": 400, "error": "MissingParameter", "message": "process_id is required"}
    if not instance:
        return {"statusCode": 400, "error": "MissingParameter", "message": "instance is required"}

    # Resolver ARN
    arn = os.environ.get("LEGACY_STEP_FUNCTION_ARN")
    if not arn:
        region = os.environ.get("DATALAKE_AWS_REGION") or os.environ.get("AWS_REGION", "us-east-2")
        account = os.environ.get("DATALAKE_AWS_ACCOUNT_ID", "832257724409")
        env = (os.environ.get("DATALAKE_ENVIRONMENT", "dev")).lower()
        arn = f"arn:aws:states:{region}:{account}:stateMachine:aje-{env}-datalake-workflow_legacy_group_{process_id}-sf"
        logger.info(f"Constructed legacy Step Function ARN: {arn}")
    else:
        logger.info(f"Using legacy Step Function ARN from env: {arn}")

    endpoints = [endpoint_name] if endpoint_name else endpoint_names
    if not endpoints:
        return {"statusCode": 400, "error": "MissingParameter", "message": "Provide endpoint_name or endpoint_names"}

    executions = []
    for ep in endpoints:
        # Input compatible con tu Pass("SetDefaults")
        input_payload = {
            "process_id": process_id,
            "instance": instance,
            "load_redshift": True,
            "load_ods": True,
            # # 
            # **({"load_redshift": load_redshift} if isinstance(load_redshift, dict) else {}),
            # **({"load_ods": load_ods} if isinstance(load_ods, dict) else {}),

            # Contexto adicional para el resto del flujo
            "endpoint_name": ep,
            "scheduled_execution": scheduled_execution,
        }

        exec_name = _build_exec_name(instance, ep, process_id)
        logger.info(f"Starting {arn} name={exec_name} input={json.dumps(input_payload)}")
        try:
            resp = sfn.start_execution(
                stateMachineArn=arn,
                name=exec_name,
                input=json.dumps(input_payload),
            )
            executions.append({"endpoint_name": ep, "execution_arn": resp["executionArn"]})
        except sfn.exceptions.ExecutionAlreadyExists as e:
            executions.append({"endpoint_name": ep, "error": "ExecutionAlreadyExists", "message": str(e)})
        except sfn.exceptions.ExecutionLimitExceeded as e:
            return {
                "statusCode": 429,
                "error": "ExecutionLimitExceeded",
                "message": str(e),
                "partial_executions": executions,
                "stateMachineArn": arn,
            }
        except sfn.exceptions.StateMachineDoesNotExist:
            return {
                "statusCode": 404,
                "error": "StateMachineDoesNotExist",
                "message": "Legacy Step Function not found",
                "stateMachineArn": arn,
            }
        except Exception as e:
            logger.exception("StartExecution failed")
            return {
                "statusCode": 500,
                "error": "InternalError",
                "message": str(e),
                "partial_executions": executions,
                "stateMachineArn": arn,
            }

    return {
        "statusCode": 200,
        "status": "RUNNING",
        "instance": instance,
        "process_id": process_id,
        "processed_endpoints": endpoints,
        "executions": executions,
        "total_executions": len(executions),
        "stateMachineArn": arn,
    }
