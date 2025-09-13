import aws_cdk as cdk
import csv
import json
from constructs import Construct
from typing import List, Dict
from aws_cdk import (
    Stack, Duration, Tags, CfnOutput,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_scheduler as scheduler
)
from aje_cdk_libs.builders.resource_builder import ResourceBuilder
from aje_cdk_libs.builders.name_builder import NameBuilder
from aje_cdk_libs.models.configs import LambdaConfig, EventBridgeSchedulerConfig
from aje_cdk_libs.models.configs import StepFunctionConfig, LambdaConfig
from aje_cdk_libs.constants.services import Services
from constants.paths import Paths
import json


class CdkDatalakeIngestBigmagicInstanceStack(Stack):
    
    def __init__(self, scope: Construct, construct_id: str, project_config, 
                 instance_name: str, endpoint_names: List[str], base_stack_outputs: Dict[str, str],
                 group_stack_references: Dict[str, Dict[str, str]], **kwargs):
        super().__init__(scope, construct_id, **kwargs)
        
        self.PROJECT_CONFIG = project_config
        self.instance_name = instance_name
        self.endpoint_names = endpoint_names
        self.base_stack_outputs = base_stack_outputs
        self.group_stack_references = group_stack_references
        
        # Initialize builders
        self.builder = ResourceBuilder(self, project_config)
        self.paths = Paths(project_config.app_config)
        self.name_builder = NameBuilder(project_config)
        
        self._create_iam_roles()
        self._create_lambda_functions()
        self._update_step_function_permissions()
        self._create_instance_step_function()
        self._create_eventbridge_schedulers()

    def _create_job_tags(self, job_type):
        tags = {
            'DataSource': self.PROJECT_CONFIG.app_config.get('datasource', ''),
            'Instance': self.instance_name,
            'Process': 'Ingest',
            'SubProcess': job_type,
            'Environment': self.PROJECT_CONFIG.environment.value,
            'Project': self.PROJECT_CONFIG.project_name,
            'ManagedBy': 'CDK'
        }
        return {k: v for k, v in tags.items() if v}
    
    def _create_iam_roles(self):
        self.lambda_role = self._create_lambda_execution_role()
        self.step_function_role = self._import_step_function_role()
    
    def _create_lambda_functions(self):
        self.lambda_functions = {}
        for endpoint_name in self.endpoint_names:
            lambda_function = self._create_invoke_step_function_lambda(endpoint_name)
            self.lambda_functions[endpoint_name] = lambda_function
            lambda_function.grant_invoke(self.step_function_role)
    
    def _update_step_function_permissions(self):
        lambda_arns = [lambda_func.function_arn for lambda_func in self.lambda_functions.values()]
        
        if lambda_arns:
            self.step_function_role.add_to_principal_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["lambda:InvokeFunction"],
                    resources=lambda_arns
                )
            )
            
            lambda_policy_name = f"LambdaInvokePolicy{self.instance_name.replace('-', '').replace('_', '').title()}"
            self.step_function_role.attach_inline_policy(
                iam.Policy(
                    self, f"LambdaInvokePolicy{self.instance_name}",
                    policy_name=lambda_policy_name,
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["lambda:InvokeFunction"],
                            resources=lambda_arns
                        )
                    ]
                )
            )

    def _import_step_function_role(self):
        step_function_role = iam.Role.from_role_arn(
            self, "StepFunctionRole",
            self.base_stack_outputs["RoleStepFunctionArn"]
        )

        # Add SNS permissions for notifications
        step_function_role.add_to_principal_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["sns:Publish"],
                resources=[
                    self.base_stack_outputs["SnsFailedTopicArn"],
                    self.base_stack_outputs["SnsSuccessTopicArn"]
                ]
            )
        )
        
        # Add Step Functions permissions for invoking other step functions
        step_function_role.add_to_principal_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "states:StartExecution",
                    "states:DescribeExecution",
                    "states:StopExecution",
                    "states:GetExecutionHistory"
                ],
                resources=[
                    f"arn:aws:states:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:stateMachine:{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.PROJECT_CONFIG.project_name}-workflow_extract_{self.PROJECT_CONFIG.app_config['datasource'].lower()}_*",
                    f"arn:aws:states:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:execution:{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.PROJECT_CONFIG.project_name}-workflow_extract_{self.PROJECT_CONFIG.app_config['datasource'].lower()}_*"
                ]
            )
        )
        
        return step_function_role
    
    def _create_instance_step_function(self):
        """
        Create the instance-level Step Function that orchestrates multiple endpoint extractions.
        Uses actual references to group step functions instead of constructing ARNs.
        """
        parallel_branches = []
        
        for endpoint_name in self.endpoint_names:
            # Find all available group step functions for this endpoint
            available_groups = {}
            for group_key, group_ref in self.group_stack_references.items():
                if group_ref['endpoint_name'] == endpoint_name:
                    available_groups[group_ref['process_id']] = group_ref
            
            if not available_groups:
                # If no group step functions exist for this endpoint, create a simple pass state
                endpoint_chain = sfn.Pass(
                    self, f"NoGroupsFor{endpoint_name.replace('_', '').replace('-', '')}",
                    comment=f"No group step functions available for endpoint {endpoint_name}",
                    result=sfn.Result.from_object({
                        "endpoint_name": endpoint_name,
                        "status": "SKIPPED",
                        "message": "No group step functions available"
                    })
                )
                parallel_branches.append(endpoint_chain)
                continue
            
            # Lambda task to prepare input
            prepare_input_task = tasks.LambdaInvoke(
                self, f"PrepareInput{endpoint_name.replace('_', '').replace('-', '')}",
                lambda_function=self.lambda_functions[endpoint_name],
                payload=sfn.TaskInput.from_object({
                    "process_id.$": "$.process_id",
                    "endpoint_name": endpoint_name,
                    "instance": self.instance_name
                }),
                result_path=f"$.{endpoint_name.replace('-', '_').replace('.', '_')}_input"
            )
            
            # Create choice state to select the appropriate group step function
            choose_group = sfn.Choice(self, f"ChooseGroup{endpoint_name.replace('_', '').replace('-', '')}")
            
            success_state = sfn.Pass(
                self, f"EndPoint{endpoint_name.replace('_', '').replace('-', '')}Success",
                comment=f"Endpoint {endpoint_name} execution completed successfully"
            )
            
            # Create step function invocation for each available process_id
            for process_id, group_ref in available_groups.items():
                # Use StepFunctionsStartExecution with actual step function reference
                invoke_group_task = tasks.StepFunctionsStartExecution(
                    self, f"InvokeGroup{endpoint_name.replace('_', '').replace('-', '')}{process_id}",
                    state_machine=group_ref['step_function'],
                    input=sfn.TaskInput.from_object({
                        "endpoint_name": endpoint_name,
                        "process_id": process_id,
                        "instance": self.instance_name,
                        "scheduled_execution": True,
                        "run_extract": True,
                    }),
                    integration_pattern=sfn.IntegrationPattern.RUN_JOB  # Synchronous execution
                )
                
                # Add error handling for group execution
                invoke_group_task.add_catch(
                    tasks.CallAwsService(
                        self, f"NotifyGroupFailure{endpoint_name.replace('_', '').replace('-', '')}{process_id}",
                        service="sns",
                        action="publish",
                        parameters={
                            "TopicArn": self.base_stack_outputs["SnsFailedTopicArn"],
                            "Message": sfn.JsonPath.format(f"Group step function execution failed for endpoint {endpoint_name}, process {process_id} in instance {self.instance_name}"),
                            "Subject": f"Instance {self.instance_name} - Group Execution Failed"
                        },
                        iam_resources=[self.base_stack_outputs["SnsFailedTopicArn"]]
                    ).next(sfn.Fail(self, f"GroupExecutionFailed{endpoint_name.replace('_', '').replace('-', '')}{process_id}", cause="Group step function execution failed")),
                    errors=["States.ALL"]
                )
                
                # Add condition to choice - using string comparison since process_id is a string
                choose_group.when(
                    sfn.Condition.string_equals("$.process_id", str(process_id)),
                    invoke_group_task.next(success_state)
                )
            
            # Default case for unknown process_id
            unknown_process_fail = sfn.Fail(
                self, f"UnknownProcess{endpoint_name.replace('_', '').replace('-', '')}",
                cause=f"Unknown process_id for endpoint {endpoint_name}. Available: {list(available_groups.keys())}",
                error="UnknownProcessId"
            )
            choose_group.otherwise(unknown_process_fail)
            
            # Add error handling for input preparation
            prepare_input_task.add_catch(
                tasks.CallAwsService(
                    self, f"NotifyInputFailure{endpoint_name.replace('_', '').replace('-', '')}",
                    service="sns",
                    action="publish",
                    parameters={
                        "TopicArn": self.base_stack_outputs["SnsFailedTopicArn"],
                        "Message": sfn.JsonPath.format(f"Input preparation failed for endpoint {endpoint_name} in instance {self.instance_name}"),
                        "Subject": f"Instance {self.instance_name} - Input Preparation Failed"
                    },
                    iam_resources=[self.base_stack_outputs["SnsFailedTopicArn"]]
                ).next(sfn.Fail(self, f"InputPreparationFailed{endpoint_name.replace('_', '').replace('-', '')}", cause="Input preparation failed")),
                errors=["States.ALL"]
            )
            
            # Chain: Prepare input -> Choose group
            endpoint_chain = prepare_input_task.next(choose_group)
            parallel_branches.append(endpoint_chain)

        # Create parallel state for all endpoints
        parallel_state = sfn.Parallel(
            self, "ParallelEndPointProcessing",
            comment=f"Process all endpoints in parallel for instance {self.instance_name}"
        )
        
        for branch in parallel_branches:
            parallel_state.branch(branch)
        
        # Add catch-all error handling for the parallel state
        parallel_state.add_catch(
            tasks.CallAwsService(
                self, "NotifyParallelFailure",
                service="sns",
                action="publish",
                parameters={
                    "TopicArn": self.base_stack_outputs["SnsFailedTopicArn"],
                    "Message": sfn.JsonPath.format(f"Parallel processing failed for instance {self.instance_name}"),
                    "Subject": f"Instance {self.instance_name} - Parallel Processing Failed"
                },
                iam_resources=[self.base_stack_outputs["SnsFailedTopicArn"]]
            ).next(sfn.Fail(self, "ParallelProcessingFailed", cause="Parallel processing failed")),
            errors=["States.ALL"]
        )
        
        # After all parallel branches complete, invoke domain step function asynchronously with error handling
        #invoke_domain_task = tasks.LambdaInvoke(
        #    self, "InvokeDomainStepFunction",
        #    lambda_function=_lambda.Function.from_function_arn(
        #        self, "DomainLambdaFunction",
        #        self.base_stack_outputs["DomainLambdaFunctionArn"]
        #    ),
        #    payload=sfn.TaskInput.from_object({
        #        "process_id.$": "$$.Execution.Input.process_id",
        #        "endpoint_name.$": "$$.Execution.Input.endpoint_name", 
        #        "instance.$": "$$.Execution.Input.instance",
        #        "scheduled_execution.$": "$$.Execution.Input.scheduled_execution",
        #        "run_extract.$": "$$.Execution.Input.run_extract",
        #        "endpoint_names": self.endpoint_names,
        #        "execution_results.$": "$"
        #    }),
        #    invocation_type=tasks.LambdaInvocationType.EVENT  # Asynchronous invocation
        #)
        #
        ## Add error handling for domain invocation
        #invoke_domain_task.add_catch(
        #    tasks.CallAwsService(
        #        self, "NotifyDomainFailure",
        #        service="sns",
        #        action="publish",
        #        parameters={
        #            "TopicArn": self.base_stack_outputs["SnsFailedTopicArn"],
        #            "Message": sfn.JsonPath.format(f"Domain step function invocation failed for instance {self.instance_name}"),
        #            "Subject": f"Instance {self.instance_name} - Domain Invocation Failed"
        #        },
        #        iam_resources=[self.base_stack_outputs["SnsFailedTopicArn"]]
        #    ).next(sfn.Pass(self, "DomainInvocationFailedButContinue", comment="Domain invocation failed but instance processing completed")),
        #    errors=["States.ALL"]
        #)

### miko
        ## After all parallel branches complete, invoke domain step function asynchronously with error handling
        #invoke_legacy_task = tasks.LambdaInvoke(
        #    self, "InvokeLegacyStepFunction",
        #    lambda_function=_lambda.Function.from_function_arn(
        #        self, "LegacyLambdaFunction",
        #        self.base_stack_outputs["LegacyLambdaFunctionArn"]
        #    ),
        #    payload=sfn.TaskInput.from_object({
        #        "process_id.$": "$$.Execution.Input.process_id",
        #        "endpoint_name.$": "$$.Execution.Input.endpoint_name", 
        #        "instance.$": "$$.Execution.Input.instance",
        #        "scheduled_execution.$": "$$.Execution.Input.scheduled_execution",
        #        "run_extract.$": "$$.Execution.Input.run_extract",
        #        "endpoint_names": self.endpoint_names,
        #        "execution_results.$": "$"
        #    }),
        #    invocation_type=tasks.LambdaInvocationType.EVENT  # Asynchronous invocation
        #)
        #
        ## Add error handling for legacy invocation
        #invoke_legacy_task.add_catch(
        #    tasks.CallAwsService(
        #        self, "NotifyLegacyFailure",
        #        service="sns",
        #        action="publish",
        #        parameters={
        #            "TopicArn": self.base_stack_outputs["SnsFailedTopicArn"],
        #            "Message": sfn.JsonPath.format(f"Legacy step function invocation failed for instance {self.instance_name}"),
        #            "Subject": f"Instance {self.instance_name} - Legacy Invocation Failed"
        #        },
        #        iam_resources=[self.base_stack_outputs["SnsFailedTopicArn"]]
        #    ).next(sfn.Pass(self, "LegacyInvocationFailedButContinue", comment="Legacy invocation failed but instance processing completed")),
        #    errors=["States.ALL"]
        #)

## end miko
        
        # Success state
        final_success = sfn.Pass(
            self, "InstanceProcessingComplete",
            comment=f"Instance {self.instance_name} processing completed successfully"
        )
        
        # Create the main step function definition
        #definition = parallel_state.next(invoke_domain_task).next(invoke_legacy_task).next(final_success)
        #definition_legacy = parallel_state.next(invoke_legacy_task).next(final_success)
        
#################################   Ejecucion en paralelo para domain y legacy
        ## 1) Bloque paralelo para llamadas post-ingesta
        #post_invocations = sfn.Parallel(
        #    self, "PostIngestionInvocations",
        #    comment=f"Invoke Domain and Legacy in parallel for instance {self.instance_name}"
        #)

        ## Cada rama es el task ya definido (con sus catches propios)
        #post_invocations.branch(invoke_domain_task)
        #post_invocations.branch(invoke_legacy_task)

        ## (Opcional) catch de nivel paralelo si quieres una notificación "global" adicional
        #post_invocations.add_catch(
        #    tasks.CallAwsService(
        #        self, "NotifyPostInvocationsFailure",
        #        service="sns",
        #        action="publish",
        #        parameters={
        #            "TopicArn": self.base_stack_outputs["SnsFailedTopicArn"],
        #            "Message": sfn.JsonPath.format(f"Post invocations (Domain/Legacy) failed for instance {self.instance_name}"),
        #            "Subject": f"Instance {self.instance_name} - Post Invocations Failed"
        #        },
        #        iam_resources=[self.base_stack_outputs["SnsFailedTopicArn"]]
        #    ).next(sfn.Pass(self, "PostInvocationsFailedButContinue", comment="Post invocations failed but instance processing completed")),
        #    errors=["States.ALL"]
        #)

        # 2) Nueva definición: endpoints en paralelo -> (Domain & Legacy) en paralelo -> success
        #definition = parallel_state.next(post_invocations).next(final_success)
        definition = parallel_state.next(final_success)
#################################   FIN Ejecucion en paralelo para domain y legacy

        # Create the Step Function
        instance_step_function_name = self.name_builder.build(
            service=Services.STEP_FUNCTION,
            descriptive_name=f"workflow_instance_{self.PROJECT_CONFIG.app_config['datasource'].lower()}_{self.instance_name}"
        )
        
        self.instance_step_function = sfn.StateMachine(
            self, "InstanceStepFunction",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            state_machine_name=instance_step_function_name,
            role=self.step_function_role
        )
        
        CfnOutput(
            self, "InstanceStepFunctionArn",
            value=self.instance_step_function.state_machine_arn,
            description=f"Step Function ARN for instance {self.instance_name}"
        )

    def _create_invoke_step_function_lambda(self, endpoint_name: str):
        short_endpoint_name = endpoint_name.replace('_', '').replace('-', '').replace('.', '')[:10]
        descriptive_name = f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_invoke_{short_endpoint_name}"
        
        lambda_config = LambdaConfig(
            function_name=descriptive_name,
            handler="lambda_function.lambda_handler",
            code_path=f"{self.paths.LOCAL_ARTIFACTS_LAMBDA_CODE_RAW}/invoke_step_function",
            timeout=Duration.minutes(15),
            runtime=_lambda.Runtime("python3.13"),
            role=self.lambda_role
        )
        
        lambda_function = self.builder.build_lambda_function(lambda_config)
        
        tags = self._create_job_tags("Lambda")
        for key, value in tags.items():
            Tags.of(lambda_function).add(key, value)
        
        return lambda_function

    def _create_domain_invocation_phase(self):
        """Create the domain step function invocation phase with error handling"""
        
        # Import domain lambda function from base stack
        domain_lambda_function = _lambda.Function.from_function_arn(
            self, "DomainLambdaFunction",
            self.base_stack_outputs["DomainLambdaFunctionArn"]
        )
        
        # Grant invoke permission to step function role
        domain_lambda_function.grant_invoke(self.step_function_role)
        
        # Prepare input for domain lambda
        prepare_domain_input = sfn.Pass(
            self, "PrepareDomainInput",
            parameters={
                "process_id.$": "$.process_id",
                "instance": self.instance_name
            },
            result_path="$.domain_input"
        )
        
        # Invoke domain lambda function synchronously
        invoke_domain_lambda = tasks.LambdaInvoke(
            self, "InvokeDomainLambda",
            lambda_function=domain_lambda_function,
            input_path="$.domain_input",
            invocation_type=tasks.LambdaInvocationType.REQUEST_RESPONSE,  # Synchronous
            result_path="$.domain_result"
        )
        
        # Add retry logic for domain lambda failures
        invoke_domain_lambda.add_retry(
            max_attempts=2,
            interval=Duration.seconds(10),
            backoff_rate=1.5,
            errors=["States.TaskFailed", "Lambda.ServiceException", "Lambda.AWSLambdaException"]
        )
        
        # Add error handling for domain lambda failures
        invoke_domain_lambda.add_catch(
            tasks.CallAwsService(
                self, "NotifyDomainFailure",
                service="sns",
                action="publish",
                parameters={
                    "TopicArn": self.base_stack_outputs["SnsFailedTopicArn"],
                    "Message": sfn.JsonPath.format(f"Domain step function invocation failed for instance {self.instance_name}"),
                    "Subject": f"Instance {self.instance_name} - Domain Invocation Failed"
                },
                iam_resources=[self.base_stack_outputs["SnsFailedTopicArn"]]
            ).next(sfn.Fail(self, "DomainInvocationFailed", cause="Domain invocation failed")),
            errors=["States.ALL"]
        )
        
        # Check domain invocation success
        check_domain_success = sfn.Choice(self, "CheckDomainSuccess")
        
        # Success notification and completion
        notify_success = tasks.CallAwsService(
            self, "NotifyInstanceSuccess",
            service="sns",
            action="publish",
            parameters={
                "TopicArn": self.base_stack_outputs["SnsSuccessTopicArn"],
                "Message": sfn.JsonPath.format(f"Instance {self.instance_name} completed successfully including domain processing"),
                "Subject": f"Instance {self.instance_name} - Completed Successfully"
            },
            iam_resources=[self.base_stack_outputs["SnsSuccessTopicArn"]]
        )
        
        domain_success = sfn.Succeed(
            self, "DomainInvocationSucceeded",
            comment="Domain step function invocation completed successfully"
        )
        
        domain_failure = sfn.Fail(
            self, "DomainInvocationReturnedError",
            cause="Domain lambda returned an error status",
            error="DomainLambdaError"
        )
        
        # Check if domain lambda execution was successful (statusCode 200)
        check_domain_success.when(
            sfn.Condition.number_equals("$.domain_result.Payload.statusCode", 200),
            notify_success.next(domain_success)
        ).otherwise(domain_failure)
        
        # Chain the domain invocation phase
        return prepare_domain_input.next(invoke_domain_lambda).next(check_domain_success)

    
    def _create_lambda_execution_role(self):
        role_name = self.name_builder.build(
            service=Services.IAM_ROLE,
            descriptive_name=f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_lambda_instance_{self.instance_name}"
        )
        
        role = iam.Role(
            self, "LambdaExecutionRole",
            role_name=role_name,
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ],
            inline_policies={
                "StepFunctionInvokePolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "states:StartExecution",
                                "states:DescribeExecution",
                                "states:GetExecutionHistory"
                            ],
                            resources=[
                                f"arn:aws:states:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:stateMachine:{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.PROJECT_CONFIG.project_name}-workflow_extract_{self.PROJECT_CONFIG.app_config['datasource'].lower()}_*",
                                f"arn:aws:states:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:execution:{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.PROJECT_CONFIG.project_name}-workflow_extract_{self.PROJECT_CONFIG.app_config['datasource'].lower()}_*",
                                f"arn:aws:states:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:stateMachine:CdkDatalakeIngestBigmagicGroupStack-*",
                                f"arn:aws:states:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:execution:CdkDatalakeIngestBigmagicGroupStack-*"
                            ]
                        )
                    ]
                )
            }
        )
        return role

    def _read_programmer_csv(self) -> List[Dict[str, str]]:
        """Read programmer.csv and return configurations for current instance and environment"""
        schedules = []
        current_env = self.PROJECT_CONFIG.environment.value.upper()
        
        try:
            csv_path = f'{self.paths.LOCAL_ARTIFACTS_CONFIGURE_CSV}/programmer.csv'
            
            with open(csv_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=';')
                for row in reader:
                    row_instance = row.get('INSTANCE', '').upper()
                    row_env = row.get('ENV', '').upper()
                    
                    # Filter by instance and environment
                    if (row_instance == self.instance_name.upper() and row_env == current_env):
                        schedules.append(row)
        except FileNotFoundError:
            print(f"Warning: programmer.csv not found at {csv_path}")
        except Exception as e:
            print(f"Warning: Error reading programmer.csv: {str(e)}")
            
        return schedules

    def _create_scheduler_role(self) -> iam.Role:
        """Create IAM role for EventBridge Scheduler to invoke Step Functions"""
        role_name = self.name_builder.build(
            service=Services.IAM_ROLE,
            descriptive_name=f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_scheduler_{self.instance_name}"
        )
        
        role = iam.Role(
            self, "SchedulerRole",
            role_name=role_name,
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
            inline_policies={
                "StepFunctionInvokePolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "states:StartExecution"
                            ],
                            resources=[
                                # Allow scheduler to invoke the instance Step Function
                                self.instance_step_function.state_machine_arn
                            ]
                        )
                    ]
                )
            }
        )
        return role

    def _create_eventbridge_schedulers(self):
        """Create EventBridge Schedulers based on programmer.csv configuration"""
        schedule_configs = self._read_programmer_csv()
        
        if not schedule_configs:
            print(f"No scheduler configurations found for instance {self.instance_name}")
            return
        
        # Create scheduler role
        scheduler_role = self._create_scheduler_role()
        
        self.schedulers = {}
        
        for config in schedule_configs:
            try:
                endpoint_name = config.get('ENDPOINT_NAME', '')
                process_id = config.get('PROCESS_ID', '')
                instance = config.get('INSTANCE','')
                
                # Build cron expression from CSV fields
                cron_expression = self._build_cron_expression(config)
                
                # Create schedule name
                schedule_name = f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_{endpoint_name.lower()}_{instance.lower()}_{process_id}_schedule"
                
                # Create input payload for the Step Function
                step_function_input = {
                    "process_id": process_id,
                    "endpoint_name": endpoint_name,
                    "instance": self.instance_name,
                    "scheduled_execution": True,
                    "run_extract": True,
                }
                
                # Create scheduler configuration
                scheduler_config = EventBridgeSchedulerConfig(
                    schedule_name=schedule_name,
                    schedule_expression=cron_expression,
                    target_arn=self.instance_step_function.state_machine_arn,
                    target_role_arn=scheduler_role.role_arn,
                    description=f"Scheduled execution for {endpoint_name} process {process_id}",
                    target_input=json.dumps(step_function_input),  # Convert to JSON string
                    timezone="UTC",
                    tags=self._create_job_tags("Scheduler")
                )
                
                # Create the scheduler
                schedule = self.builder.build_eventbridge_scheduler(scheduler_config)
                self.schedulers[f"{endpoint_name}_{process_id}"] = schedule
                
                # Output the scheduler ARN
                CfnOutput(
                    self, f"Scheduler{endpoint_name}{process_id}Arn",
                    value=schedule.ref,
                    description=f"EventBridge Scheduler for {endpoint_name} process {process_id}"
                )
                
                print(f"Created scheduler: {schedule_name} with cron: {cron_expression}")
                
            except Exception as e:
                print(f"Error creating scheduler for {config}: {str(e)}")
                continue

    def _build_cron_expression(self, config: Dict[str, str]) -> str:
        """Build AWS EventBridge cron expression from CSV configuration"""
        # Get cron fields from config
        minutes = config.get('CRON_MINUTES', '*')
        hours = config.get('CRON_HOURS', '*')
        day_of_month = config.get('CRON_DAY_OF_MONTH', '*')
        month = config.get('CRON_MONTH', '*')
        day_of_week = config.get('CRON_DAY_OF_WEEK', '?')
        year = config.get('CRON_YEAR', '*')
        
        # EventBridge uses 6-field cron: minute hour day-of-month month day-of-week year
        # If day_of_week is *, change it to ? (EventBridge requirement)
        if day_of_week == '*':
            day_of_week = '?'
        
        cron_expression = f"cron({minutes} {hours} {day_of_month} {month} {day_of_week} {year})"
        return cron_expression
