import aws_cdk as cdk
from constructs import Construct
from typing import List, Dict
from aws_cdk import (
    Stack, Duration,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_iam as iam,
    aws_lambda as _lambda
)
from aje_cdk_libs.builders.resource_builder import ResourceBuilder
from aje_cdk_libs.builders.name_builder import NameBuilder
from aje_cdk_libs.models.configs import StepFunctionConfig, LambdaConfig
from aje_cdk_libs.constants.services import Services
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
        self.name_builder = NameBuilder(project_config)
        
        self._create_iam_roles()
        self._create_lambda_functions()
        self._update_step_function_permissions()
        self._create_instance_step_function()

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
                    "group_step_function_arn.$": f"States.Format('arn:aws:states:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:stateMachine:{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.PROJECT_CONFIG.project_name}-{self.PROJECT_CONFIG.app_config['datasource'].lower()}_orchestrate_extract_{endpoint_name.lower()}_{{}}-sf', $.process_id)",
                    "process_id.$": "$.process_id",
                    "endpoint_name": endpoint_name,
                    "run_extract": False,
                    "instance": self.instance_name,
                    "expected_step_function_name.$": f"States.Format('{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.PROJECT_CONFIG.project_name}-{self.PROJECT_CONFIG.app_config['datasource'].lower()}_orchestrate_extract_{endpoint_name.lower()}_{{}}-sf', $.process_id)"
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
            descriptive_name=f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_orchestrate_instance_{self.instance_name}"
        )
        
        self.instance_step_function = sfn.StateMachine(
            self, "InstanceStepFunction",
            definition=definition,
            state_machine_name=instance_step_function_name,
            role=self.step_function_role
        )
        
        from aws_cdk import CfnOutput
        CfnOutput(
            self, "InstanceStepFunctionArn",
            value=self.instance_step_function.state_machine_arn,
            description=f"Step Function ARN for instance {self.instance_name}"
        )

    def _create_invoke_step_function_lambda(self, db_name: str):
        short_db_name = db_name.replace('_', '').replace('-', '').replace('.', '')[:10]
        descriptive_name = f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_invoke_{short_db_name}"
        
        lambda_script_path = "artifacts/aws-lambda/invoke_step_function"
        
        lambda_config = LambdaConfig(
            function_name=descriptive_name,
            handler="invoke_step_function.lambda_handler",
            code_path=lambda_script_path,
            timeout=Duration.minutes(15),
            runtime=_lambda.Runtime.PYTHON_3_9,
            role=self.lambda_role
        )
        
        lambda_function = self.builder.build_lambda_function(lambda_config)
        
        tags = self._create_job_tags("Lambda")
        for key, value in tags.items():
            cdk.Tags.of(lambda_function).add(key, value)
        
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
                                f"arn:aws:states:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:stateMachine:{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.PROJECT_CONFIG.project_name}-{self.PROJECT_CONFIG.app_config['datasource'].lower()}_orchestrate_extract_*",
                                f"arn:aws:states:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:execution:{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.PROJECT_CONFIG.project_name}-{self.PROJECT_CONFIG.app_config['datasource'].lower()}_orchestrate_extract_*",
                                f"arn:aws:states:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:stateMachine:CdkDatalakeIngestBigmagicGroupStack-*",
                                f"arn:aws:states:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:execution:CdkDatalakeIngestBigmagicGroupStack-*"
                            ]
                        )
                    ]
                )
            }
        )
        return role
