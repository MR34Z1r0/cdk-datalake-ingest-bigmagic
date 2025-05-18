from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    CfnOutput,
    aws_lambda_event_sources as lambda_event_sources,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_iam as iam,
    aws_secretsmanager as secretsmanager,
    aws_s3_notifications as s3n,
    aws_apigateway as apigw,
    aws_glue_alpha as glue,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks
)
from constructs import Construct
from aje_cdk_libs.builders.resource_builder import ResourceBuilder
from aje_cdk_libs.models.configs import *
from aje_cdk_libs.constants.environments import Environments
from constants.paths import Paths
from constants.layers import Layers
import os
from dotenv import load_dotenv
import urllib.parse
from aje_cdk_libs.constants.project_config import ProjectConfig

class CdkDatalakeIngestBigMagicStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, project_config: ProjectConfig, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)         
        self.PROJECT_CONFIG = project_config        
        self.builder = ResourceBuilder(self, self.PROJECT_CONFIG)
        self.Paths = Paths(self.PROJECT_CONFIG.app_config)
        self.Layers = Layers(self.PROJECT_CONFIG.app_config, project_config.region_name, project_config.account_id)
        self.TEAM = self.PROJECT_CONFIG.app_config["team"]
        self.DATASOURCE = self.PROJECT_CONFIG.app_config["datasource"]
        
        self.import_s3_buckets()
        self.import_dynamodb_tables()
        self.import_sns_topics()
        self.deployment_s3_buckets()
        self.create_lambda_layers()
        self.create_lambdas()
        self.create_glue_jobs()
        self.create_step_functions()
        
    def import_s3_buckets(self):
        """Import an existing S3 bucket"""
        self.s3_artifacts_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["artifacts"])
        self.s3_landing_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["landing"])
        self.s3_raw_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["raw"])
        self.s3_stage_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["stage"])
        self.s3_analytics_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["analytics"])
    
    def import_dynamodb_tables(self):
        """Import an existing DynamoDB table"""
        self.dynamodb_configuration_table = self.builder.import_dynamodb_table(self.PROJECT_CONFIG.app_config["dynamodb_tables"]["configuration"])
        self.dynamodb_credentials_table = self.builder.import_dynamodb_table(self.PROJECT_CONFIG.app_config["dynamodb_tables"]["credentials"])
        self.dynamodb_columns_specifications_table = self.builder.import_dynamodb_table(self.PROJECT_CONFIG.app_config["dynamodb_tables"]["columns-specifications"])
        self.dynamodb_logs_table = self.builder.import_dynamodb_table(self.PROJECT_CONFIG.app_config["dynamodb_tables"]["logs"])
        
    def deployment_s3_buckets(self):
        """Import an existing S3 bucket"""
        
        resource_name = "raw"
        config = S3DeploymentConfig(
            f"BucketDeploymentJobsGlueCode{resource_name}",
            [s3_deployment.Source.asset(f"{self.Paths.LOCAL_ARTIFACTS_GLUE_CODE}/{resource_name}")],
            self.s3_artifacts_bucket,
            f"{self.Paths.AWS_ARTIFACTS_GLUE_CODE}/{resource_name}"
        )
        
        self.builder.deploy_s3_bucket(config)
        
        resource_name = "stage"
        config = S3DeploymentConfig(
            f"BucketDeploymentJobsGlueCode{resource_name}",
            [s3_deployment.Source.asset(f"{self.Paths.LOCAL_ARTIFACTS_GLUE_CODE}/{resource_name}")],
            self.s3_artifacts_bucket,
            f"{self.Paths.AWS_ARTIFACTS_GLUE_CODE}/{resource_name}"
        )
        
        self.builder.deploy_s3_bucket(config)
    
    def import_sns_topics(self):
        """Import an existing SNS topic"""
        self.sns_failed_topic = self.builder.import_sns_topic(self.PROJECT_CONFIG.app_config["topic_notifications"]["failed"])
        self.sns_success_topic = self.builder.import_sns_topic(self.PROJECT_CONFIG.app_config["topic_notifications"]["success"])
    
    def create_lambda_layers(self):
        """Create or reference required Lambda layers"""
        self.lambda_layer_pyodbc = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "LambdaPyodbcLayer",
            layer_version_arn=self.Layers.AWS_LAMBDA_LAYERS.get("layer_pyodbc")
        )
        
    def create_lambdas(self):
        """Create a lambda function definition for the Datalake Ingest BigMagic stack"""
        
        function_name = "get_endpoint"
        lambda_config = LambdaConfig(
            function_name=function_name,
            handler=f"{function_name}/lambda_function.lambda_handler",
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE_STAGE}",
            runtime=_lambda.Runtime.PYTHON_3_11,
            memory_size=512,
            timeout=Duration.seconds(30)
        )
        self.lambda_get_endpoint = self.builder.build_lambda_function(lambda_config)
         
        function_name = "prepare_dms_creation_task"
        lambda_config = LambdaConfig(
            function_name=function_name,
            handler=f"{function_name}/lambda_function.lambda_handler",
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE_STAGE}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            memory_size=512,
            timeout=Duration.seconds(30)
        )
        self.lambda_prepare_dms_creation_task = self.builder.build_lambda_function(lambda_config)


        
    def create_glue_jobs(self):
        """Create a job definition for the Datalake Ingest BigMagic stack"""
        
        default_arguments={
            '--S3_RAW_PREFIX': f"s3://{self.s3_raw_bucket.bucket_name}/",
            '--S3_STAGE_PREFIX': f"s3://{self.s3_stage_bucket.bucket_name}/",
            '--TOPIC_ARN': self.sns_failed_topic.topic_arn,
            '--PROJECT_NAME' : self.PROJECT_CONFIG.project_name,
            '--TEAM' : self.PROJECT_CONFIG.app_config["team"],
            '--DATA_SOURCE' : self.PROJECT_CONFIG.app_config["datasource"],
            '--DYNAMO_CONFIG_TABLE': self.dynamodb_configuration_table.table_name,
            '--DYNAMO_ENDPOINT_TABLE': self.dynamodb_credentials_table.table_name,
            '--DYNAMO_STAGE_COLUMNS': self.dynamodb_columns_specifications_table.table_name,
            '--DYNAMO_LOGS_TABLE': self.dynamodb_logs_table.table_name,
            '--TABLE_NAME': "NONE",
            '--enable-continuous-log-filter': "true",
            '--datalake-formats': "delta",
            }
        
        config = RoleConfig(
            role_name=f"crawler_stage",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            inline_policies={
                'AccessStageS3': iam.PolicyDocument(
                    statements=[iam.PolicyStatement(
                        actions=["s3:PutObject", "s3:GetObject"],
                        resources=[self.s3_stage_bucket.bucket_arn, self.s3_stage_bucket.bucket_arn + "/*"]
                    )]
                )},
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole')]
        )

        self.role_crawler_stage = self.builder.build_role(config)
        
        job_name="extract_data_bigmagic_shell"
        config = GlueJobConfig(
            job_name=job_name,
            executable=glue.JobExecutable.python_shell(
                glue_version=glue.GlueVersion.V1_0,
                python_version=glue.PythonVersion.THREE,
                script=glue.Code.from_asset(f"{self.Paths.LOCAL_ARTIFACTS_GLUE_CODE_RAW}/{job_name}.py")
            ),
            default_arguments=default_arguments,
            continuous_logging=glue.ContinuousLoggingProps(enabled=True),
            timeout=Duration.minutes(60),
            max_concurrent_runs=200
        )
        
        self.job_extract_data_bigmagic = self.builder.build_glue_job(config)
        
        job_name="light_transform"
        config = GlueJobConfig(
            job_name=job_name,
            executable=glue.JobExecutable.python_etl(
                glue_version=glue.GlueVersion.V4_0,
                python_version=glue.PythonVersion.THREE,
                script=glue.Code.from_asset(f"{self.Paths.LOCAL_ARTIFACTS_GLUE_CODE_STAGE}/{job_name}.py")
            ),
            default_arguments=default_arguments,
            worker_type=glue.WorkerType.G_1_X,
            worker_count=2,
            continuous_logging=glue.ContinuousLoggingProps(enabled=True),
            timeout=Duration.hours(3),
            max_concurrent_runs=200
        )
        
        self.job_light_transform = self.builder.build_glue_job(config)

        job_name="crawler_stage"
        config = GlueJobConfig(
            job_name=job_name,
            executable=glue.JobExecutable.python_etl(
                glue_version=glue.GlueVersion.V4_0,
                python_version=glue.PythonVersion.THREE,
                script=glue.Code.from_asset(f"{self.Paths.LOCAL_ARTIFACTS_GLUE_CODE_STAGE}/{job_name}.py")
            ),
            default_arguments=default_arguments,
            worker_type=glue.WorkerType.G_1_X,
            worker_count=2,
            continuous_logging=glue.ContinuousLoggingProps(enabled=True),
            timeout=Duration.hours(3),
            max_concurrent_runs=200
        )
        
        self.job_crawler_stage = self.builder.build_glue_job(config)
        
        self.s3_raw_bucket.grant_read_write(self.job_extract_data_bigmagic)
        self.dynamodb_configuration_table.grant_read_write_data(self.job_extract_data_bigmagic)
        self.dynamodb_credentials_table.grant_read_write_data(self.job_extract_data_bigmagic)
        self.dynamodb_columns_specifications_table.grant_read_write_data(self.job_extract_data_bigmagic)
        self.dynamodb_logs_table.grant_read_write_data(self.job_extract_data_bigmagic)
        self.sns_failed_topic.grant_publish(self.job_extract_data_bigmagic)
        
        self.s3_raw_bucket.grant_read_write(self.job_light_transform)
        self.s3_stage_bucket.grant_read_write(self.job_light_transform)
        self.dynamodb_configuration_table.grant_read_write_data(self.job_light_transform)
        self.dynamodb_credentials_table.grant_read_write_data(self.job_light_transform)
        self.dynamodb_columns_specifications_table.grant_read_write_data(self.job_light_transform)
        self.dynamodb_logs_table.grant_read_write_data(self.job_light_transform)
        self.sns_failed_topic.grant_publish(self.job_light_transform)
        
        self.s3_stage_bucket.grant_read_write(self.job_crawler_stage)
        self.dynamodb_configuration_table.grant_read_write_data(self.job_crawler_stage)
        self.dynamodb_credentials_table.grant_read_write_data(self.job_crawler_stage)
        self.dynamodb_columns_specifications_table.grant_read_write_data(self.job_crawler_stage)
        self.dynamodb_logs_table.grant_read_write_data(self.job_crawler_stage)
        self.sns_failed_topic.grant_publish(self.job_crawler_stage)

        self.s3_stage_bucket.grant_read_write(self.lambda_get_endpoint)
        self.dynamodb_configuration_table.grant_read_write_data(self.lambda_get_endpoint)
        self.dynamodb_credentials_table.grant_read_write_data(self.lambda_get_endpoint)
        self.dynamodb_columns_specifications_table.grant_read_write_data(self.lambda_get_endpoint)
        self.dynamodb_logs_table.grant_read_write_data(self.lambda_get_endpoint)
        self.sns_failed_topic.grant_publish(self.lambda_get_endpoint)
        
        self.s3_stage_bucket.grant_read_write(self.lambda_prepare_dms_creation_task)
        self.dynamodb_configuration_table.grant_read_write_data(self.lambda_prepare_dms_creation_task)
        self.dynamodb_credentials_table.grant_read_write_data(self.lambda_prepare_dms_creation_task)
        self.dynamodb_columns_specifications_table.grant_read_write_data(self.lambda_prepare_dms_creation_task)
        self.dynamodb_logs_table.grant_read_write_data(self.lambda_prepare_dms_creation_task)
        self.sns_failed_topic.grant_publish(self.lambda_prepare_dms_creation_task)

    def create_step_functions(self):
        """Crear una definición de Step Function para el flujo de trabajo de Ingesta de Datalake"""
        
        # Estados de decisión
        needs_replication_instance = sfn.Choice(self, "Needs Replication Instance?")
        
        # Estado para agregar un ARN vacío
        add_void_arn = sfn.Pass(
            self, "add void arn",
            result=sfn.Result.from_object({}),
            result_path="$.replication_instance_arn"
        )
        
        # Estado para preparación de creación de tareas
        prepare_for_task_creation = tasks.LambdaInvoke(
            self, "Prepare for Task Creation",
            lambda_function=self.lambda_prepare_dms_creation_task,
            result_path="$",
            output_path="$.Payload"
        )
        
        prepare_for_task_creation.add_retry(
            errors=["Lambda.ClientExecutionTimeoutException", "Lambda.ServiceException", 
                    "Lambda.AWSLambdaException", "Lambda.SdkClientException"],
            interval=cdk.Duration.seconds(2),
            max_attempts=6,
            backoff_rate=2
        )
        
        # Estado para obtener endpoint
        get_endpoint = tasks.LambdaInvoke(
            self, "Get Endpoint",
            lambda_function=self.lambda_get_endpoint
        )
        
        # Estado de error para el trabajo raw
        error_raw_job = sfn.Pass(
            self, "error raw job",
            # Aseguramos un output consistente incluso en caso de error
            result_path="$.glue_error_result",
            output_path="$"
        )
        
        # Estado para job raw de Glue
        raw_job = tasks.GlueStartJobRun(
            self, "raw job",
            glue_job_name="sofia-dev-datalake-extract_data_bigmagic-job",
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--TABLE_NAME.$": "$.dynamodb_key.table"
            }),
            result_path="$.glue_result"
        )
        
        raw_job.add_retry(
            errors=["Glue.AWSGlueException", "Glue.ConcurrentRunsExceededException"],
            max_attempts=10,
            backoff_rate=5
        )
        
        raw_job.add_catch(
            errors=["States.TaskFailed"],
            handler=error_raw_job
        )
        
        # Estado para job de transformación ligera
        stage_job_by_glue = tasks.GlueStartJobRun(
            self, "stage job by glue",
            glue_job_name="sofia-dev-datalake-light_transform-job",
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--TABLE_NAME.$": "$.dynamodb_key.table"
            }),
            result_path="$.stage_job_result"
        )
        
        stage_job_by_glue.add_retry(
            errors=["Glue.AWSGlueException", "Glue.ConcurrentRunsExceededException"],
            max_attempts=10,
            backoff_rate=5
        )
        
        # Añadimos un estado Pass para normalizar el output cuando execute_raw es false
        normalize_output_when_false = sfn.Pass(
            self, "normalize output when false",
            parameters={
                # Agregar un campo glue_result simulado para mantener estructura consistente
                "glue_result": {
                    "JobRunId": "N/A-skipped",
                    "Status": "SKIPPED"
                },
                # Mantener todos los demás campos del estado
                "dynamodb_key.$": "$.dynamodb_key",
                "replication_instance_arn.$": "$.replication_instance_arn",
                "process.$": "$.process",
                "execute_raw.$": "$.execute_raw",
                "stage_job_result.$": "$.stage_job_result"
            }
        )
        
        # Estado para job de crawler
        crawler_job = tasks.GlueStartJobRun(
            self, "crawler job",
            glue_job_name="sofia-dev-datalake-crawler_stage-job",
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--INPUT_ENDPOINT.$": "$.endpoint",
                "--PROCESS_ID.$": "$.process_id"
            })
        )
        
        crawler_job.add_retry(
            errors=["States.ALL"],
            max_attempts=10,
            backoff_rate=5
        )
        
        # Estado para verificar necesidad de Glue
        needs_glue = sfn.Choice(self, "needs glue?")
        
        # Estado para verificar execute_raw
        check_execute_raw = sfn.Choice(self, "Check Execute Raw")
        
        # Estado para estandarizar el output final del mapa interno
        standardize_map_output = sfn.Pass(
            self, "standardize map output",
            result_path=sfn.JsonPath.DISCARD  # Mantener la entrada intacta
        )
        
        # Configurar el flujo para Check Execute Raw
        check_execute_raw.when(
            sfn.Condition.boolean_equals("$.execute_raw", False),
            # Cuando execute_raw es false, pasamos por el normalizador y luego al stage job
            normalize_output_when_false.next(stage_job_by_glue)
        ).otherwise(raw_job)
        
        # Definir transiciones para needs_glue
        needs_glue.when(
            sfn.Condition.string_equals("$.dynamodb_key.type", "needs_glue"),
            check_execute_raw
        )
        
        # Conectar flujo de trabajo para raw_job y stage_job
        raw_job.next(stage_job_by_glue)
        
        # Ambos caminos (raw job o stage job directo) deben pasar por el estandarizador antes de terminar
        stage_job_by_glue.next(standardize_map_output)
        error_raw_job.next(stage_job_by_glue)
        
        # Crear el primer estado Pass para el Map interno
        pass_internal = sfn.Pass(
            self, "Pass Internal"
        )
        
        # Map interno para necesidades de consultas - sin pasar iterator directamente
        needs_query_map = sfn.Map(
            self, "Needs Query?",
            max_concurrency=15,
            items_path="$.dynamodb_key",
            parameters={
                "dynamodb_key.$": "$$.Map.Item.Value",
                "replication_instance_arn.$": "$.replication_instance_arn",
                "process.$": "$.process",
                "execute_raw.$": "$.execute_raw"
            },
            result_selector={
                # Definimos explícitamente qué campos queremos en el output del map
                "result.$": "$[*]",
                "status": "COMPLETED"
            },
            result_path="$.query_results"  # Guardamos el resultado en un path específico
        )
        
        # Definir el iterator después de crear el Map
        needs_query_map.iterator(needs_glue)
        
        # Conectar el flujo para needs_query_map
        needs_query_map.next(get_endpoint)
        get_endpoint.next(crawler_job)
        
        # Crear el estado Pass para el Map State exterior
        pass_state = sfn.Pass(
            self, "Pass",
            result=sfn.Result.from_string("SUCCEEDED"),
            result_path="$.result"
        )
        
        # Conectar Pass con Prepare for Task Creation
        pass_state.next(prepare_for_task_creation)
        prepare_for_task_creation.next(needs_query_map)
        
        # Map exterior para procesar todas las tablas - sin pasar iterator directamente
        map_state = sfn.Map(
            self, "Map State",
            max_concurrency=1,
            items_path="$.table_names",
            parameters={
                "dynamodb_key.$": "$$.Map.Item.Value",
                "replication_instance_arn.$": "$.replication_instance_arn",
                "bd_type.$": "$.bd_type",
                "process.$": "$.process",
                "execute_raw.$": "$.execute_raw"
            },
            result_path=sfn.JsonPath.DISCARD
        )
        
        # Definir el iterator después de crear el Map
        map_state.iterator(pass_state)
        
        # Configurar el flujo principal
        needs_replication_instance.when(
            sfn.Condition.boolean_equals("$.needs_dms", True),
            map_state
        ).otherwise(add_void_arn)
        
        add_void_arn.next(map_state)
        
        # Definir el flujo de trabajo completo
        definition = needs_replication_instance
        
        # Crear la máquina de estados con timeout
        state_machine = sfn.StateMachine(
            self, "DatalakeIngestionWorkflow",
            definition=definition,
            timeout=cdk.Duration.seconds(3600)
        )
        
        return state_machine