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
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE}/stage",
            runtime=_lambda.Runtime.PYTHON_3_11,
            memory_size=512,
            timeout=Duration.seconds(30)
        )
        self.get_endpoint_lambda = self.builder.build_lambda_function(lambda_config)
         
        function_name = "update_load_start_value_mysql"
        lambda_config = LambdaConfig(
            function_name=function_name,
            handler=f"{function_name}/lambda_function.lambda_handler",
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE}/stage",
            runtime=_lambda.Runtime.PYTHON_3_9,
            memory_size=512,
            timeout=Duration.seconds(30)
        )
        self.update_load_start_value_mysql_lambda = self.builder.build_lambda_function(lambda_config)
        
        function_name = "update_load_start_value_oracle"
        lambda_config = LambdaConfig(
            function_name=function_name,
            handler=f"{function_name}/lambda_function.lambda_handler",
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE}/stage",
            runtime=_lambda.Runtime.PYTHON_3_9,
            memory_size=512,
            timeout=Duration.seconds(30)
        )
        self.update_load_start_value_oracle_lambda = self.builder.build_lambda_function(lambda_config)
                
        function_name = "prepare_dms_creation_task"
        lambda_config = LambdaConfig(
            function_name=function_name,
            handler=f"{function_name}/lambda_function.lambda_handler",
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE}/stage",
            runtime=_lambda.Runtime.PYTHON_3_9,
            memory_size=512,
            timeout=Duration.seconds(30)
        )
        self.prepare_dms_creation_task_lambda = self.builder.build_lambda_function(lambda_config)
        
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
            '--enable-continuous-log-filter': "true",
            '--TABLE_NAME': "NONE",
            '--datalake-formats': "delta",
            }
        
        # Crawler role for stage
        role_crawler = iam.Role(self, f"aje-{props['Environment']}-stage-role-crawler",
            assumed_by=iam.ServicePrincipal(
                "glue.amazonaws.com"),
            inline_policies={
                'AccessStageS3': iam.PolicyDocument(
                    statements=[iam.PolicyStatement(
                        actions=["s3:PutObject", "s3:GetObject"],
                        resources=[self.s3_stage_bucket.bucket_arn, self.s3_stage_bucket.bucket_arn + "/*"]
                    )]
                )},
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole')]
            )
        
        config = GlueJobConfig(
            job_name="extract_data_bigmagic",
            executable=glue.JobExecutable.python_shell(
                glue_version=glue.GlueVersion.V1_0,
                python_version=glue.PythonVersion.THREE,
                script=glue.Code.from_asset(f"{self.Paths.LOCAL_ARTIFACTS_GLUE_CODE}/raw/extract_data_bigmagic_spark.py")
            ),
            default_arguments=default_arguments,
            continuous_logging=glue.ContinuousLoggingProps(enabled=True),
            timeout=Duration.minutes(60),
            max_concurrent_runs=200
        )
        
        self.job_extract_data_bigmagic = self.builder.build_glue_job(config)
        
        config = GlueJobConfig(
            job_name="light_transform",
            executable=glue.JobExecutable.python_etl(
                glue_version=glue.GlueVersion.V4_0,
                python_version=glue.PythonVersion.THREE,
                script=glue.Code.from_asset(f"{self.Paths.LOCAL_ARTIFACTS_GLUE_CODE}/stage/light_transform.py")
            ),
            default_arguments=default_arguments,
            worker_type=glue.WorkerType.G_1_X,
            worker_count=2,
            continuous_logging=glue.ContinuousLoggingProps(enabled=True),
            timeout=Duration.hours(3),
            max_concurrent_runs=200
        )
        
        self.job_light_transform = self.builder.build_glue_job(config)

        config = GlueJobConfig(
            job_name="crawler_stage",
            executable=glue.JobExecutable.python_etl(
                glue_version=glue.GlueVersion.V4_0,
                python_version=glue.PythonVersion.THREE,
                script=glue.Code.from_asset(f"{self.Paths.LOCAL_ARTIFACTS_GLUE_CODE}/stage/crawlers_job.py")
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
        
    def create_step_functions(self):
        """Create a step function definition for the Datalake Ingestion workflow"""
        
        # Estados de decisión
        needs_replication_instance = sfn.Choice(self, "Needs Replication Instance?")
        
        # Estado para agregar un ARN vacío
        add_void_arn = sfn.Pass(
            self, "add void arn",
            result=sfn.Result.from_object({}),
            result_path="$.replication_instance_arn"
        )
        
        # Estado final - eliminar instancia de replicación
        delete_replication_instance = tasks.LambdaInvoke(
            self, "Delete Replication Instance",
            lambda_function=self.delete_dms_replication_task_lambda
        )
        
        # Configurar SNS para notificación de fallos
        record_failed = tasks.SnsPublish(
            self, "Record Failed",
            message=sfn.TaskInput.from_object({
                "message": "failed preparing load",
                "error.$": "$.error"
            }),
            topic=self.sns_failed_topic,
            result_path=sfn.JsonPath.DISCARD
        )
        
        # Estado para actualizar valor de inicio para tablas SQL
        update_start_value_sql = tasks.LambdaInvoke(
            self, "Update start value for table SQL",
            lambda_function=self.update_load_start_value_mssql_lambda
        )
        
        update_start_value_sql.add_catch(
            errors=["States.ALL"],
            result_path="$.error",
            handler=record_failed
        )
        
        # Estado de decisión para verificar si el valor inicial se actualizó
        start_value_updated = sfn.Choice(self, "Start Value Updated?")
        
        # Estado de preparación para creación de tareas
        prepare_for_task_creation = tasks.LambdaInvoke(
            self, "Prepare for Task Creation",
            lambda_function=self.prepare_dms_creation_task_lambda
        )
        
        # Estado para obtener endpoint
        get_endpoint = tasks.LambdaInvoke(
            self, "Get Endpoint",
            lambda_function=self.get_endpoint_lambda
        )
        
        # Trabajos de Glue
        # Estado de error para el trabajo raw
        error_raw_job = sfn.Pass(
            self, "error raw job"
        )
        
        # Configuración de trabajos Glue
        raw_job = tasks.GlueStartJobRun(
            self, "raw job",
            glue_job_name=self.job_extract_data_bigmagic.job_name,
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
        
        stage_job_by_glue = tasks.GlueStartJobRun(
            self, "stage job by glue",
            glue_job_name=self.job_light_transform.job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--TABLE_NAME.$": "$.dynamodb_key.table"
            })
        )
        
        stage_job_by_glue.add_retry(
            errors=["Glue.AWSGlueException", "Glue.ConcurrentRunsExceededException"],
            max_attempts=10,
            backoff_rate=5
        )
        
        crawler_job = tasks.GlueStartJobRun(
            self, "crawler job",
            glue_job_name=self.job_crawler_stage.job_name,
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
        
        # Map interno para necesidades de consultas
        needs_query_map = sfn.Map(
            self, "Needs Query?",
            max_concurrency=15,
            items_path="$.dynamodb_key",
            parameters={
                "dynamodb_key.$": "$$.Map.Item.Value",
                "replication_instance_arn.$": "$.replication_instance_arn",
                "process.$": "$.process",
                "execute_raw.$": "$.execute_raw"  # Añadir el parámetro execute_raw
            }
        )
        
        # Choice para necesidades de Glue
        needs_glue = sfn.Choice(self, "needs glue?")
        
        # Nuevo Choice para verificar execute_raw
        check_execute_raw = sfn.Choice(self, "Check Execute Raw")
        
        # Configurar el flujo para los needs_glue y check_execute_raw
        needs_glue.when(
            sfn.Condition.string_equals("$.dynamodb_key.type", "needs_glue"),
            check_execute_raw
        )
        
        # Configurar check_execute_raw para saltar raw_job si execute_raw es false
        check_execute_raw.when(
            sfn.Condition.boolean_equals("$.execute_raw", False),
            stage_job_by_glue  # Si execute_raw es false, va directo a stage_job_by_glue
        ).otherwise(raw_job)  # De lo contrario, va a raw_job
        
        raw_job.next(stage_job_by_glue)
        
        needs_query_map.iterator(needs_glue)
        needs_query_map.next(get_endpoint)
        get_endpoint.next(crawler_job)
        
        # Map exterior para procesar todas las tablas
        map_state = sfn.Map(
            self, "Map State",
            max_concurrency=1,
            items_path="$.table_names",
            parameters={
                "dynamodb_key.$": "$$.Map.Item.Value",
                "replication_instance_arn.$": "$.replication_instance_arn",
                "bd_type.$": "$.bd_type",
                "process.$": "$.process",
                "execute_raw.$": "$.execute_raw"  # Añadir el parámetro execute_raw
            },
            result_path=sfn.JsonPath.DISCARD
        )
        
        # Construir el flujo para el Map exterior
        update_start_value_sql.next(start_value_updated)
        
        start_value_updated.when(
            sfn.Condition.string_equals("$.result", "SUCCEEDED"),
            prepare_for_task_creation
        ).otherwise(record_failed)
        
        prepare_for_task_creation.next(needs_query_map)
        
        map_state.iterator(update_start_value_sql)
        map_state.next(delete_replication_instance)
        
        # Configurar el flujo principal
        needs_replication_instance.when(
            sfn.Condition.boolean_equals("$.needs_dms", True),
            map_state
        ).otherwise(add_void_arn)
        
        add_void_arn.next(map_state)
        
        # Definir el flujo de trabajo completo
        definition = needs_replication_instance
        
        # Crear la máquina de estados
        config = StepFunctionConfig(
            name="light_transform_bigmagic",
            definition=definition
        )
        
        self.state_function = self.builder.build_step_function(config)
        
        