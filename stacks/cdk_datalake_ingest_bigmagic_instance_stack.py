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
        return iam.Role.from_role_arn(
            self, "StepFunctionRole",
            self.base_stack_outputs["RoleStepFunctionArn"]
        )
    
    def _create_instance_step_function(self):
        parallel_branches = []
        
        for endpoint_name in self.endpoint_names:
            construct_arn_task = sfn.Pass(
                self, f"ConstructArn{endpoint_name.replace('_', '').replace('-', '')}",
                parameters={
                    "group_step_function_arn.$": f"States.Format('arn:aws:states:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:stateMachine:{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.PROJECT_CONFIG.project_name}-workflow_extract_{self.PROJECT_CONFIG.app_config['datasource'].lower()}_{endpoint_name.lower()}_{{}}-sf', $.process_id)",
                    "process_id.$": "$.process_id",
                    "endpoint_name": endpoint_name,
                    "run_extract": False, #False si no tiene una conexión directa con el endpoint desde AWS
                    "instance": self.instance_name,
                    "expected_step_function_name.$": f"States.Format('{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.PROJECT_CONFIG.project_name}-workflow_extract_{self.PROJECT_CONFIG.app_config['datasource'].lower()}_{endpoint_name.lower()}_{{}}-sf', $.process_id)"
                },
                result_path=f"$.{endpoint_name.replace('-', '_').replace('.', '_')}_config"
            )
            
            invoke_lambda = self.lambda_functions[endpoint_name]
            
            invoke_endpoint_name_task = tasks.LambdaInvoke(
                self, f"InvokeEndPoint{endpoint_name.replace('_', '').replace('-', '')}",
                lambda_function=invoke_lambda,
                input_path=f"$.{endpoint_name.replace('-', '_').replace('.', '_')}_config",
                result_path=f"$.{endpoint_name.replace('-', '_').replace('.', '_')}_result"
            )
            
            endpoint_chain = construct_arn_task.next(invoke_endpoint_name_task)
            parallel_branches.append(endpoint_chain)

        if len(parallel_branches) > 1:
            parallel_state = sfn.Parallel(
                self, "ParallelEndPointProcessing",
                comment=f"Process all endpoints in parallel for instance {self.instance_name}"
            )
            
            for branch in parallel_branches:
                parallel_state.branch(branch)
            
            definition = parallel_state
        elif len(parallel_branches) == 1:
            definition = parallel_branches[0]
        else:
            definition = sfn.Pass(
                self, "NoProcesses",
                comment=f"No endpoints found for instance {self.instance_name}"
            )
        
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
            runtime=_lambda.Runtime.PYTHON_3_9,
            role=self.lambda_role
        )
        
        lambda_function = self.builder.build_lambda_function(lambda_config)
        
        tags = self._create_job_tags("Lambda")
        for key, value in tags.items():
            Tags.of(lambda_function).add(key, value)
        
        return lambda_function
    
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
                    "scheduled_execution": True
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
