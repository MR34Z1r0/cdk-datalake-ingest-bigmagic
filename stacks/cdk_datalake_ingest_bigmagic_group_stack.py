from aws_cdk import Stack, Duration, aws_stepfunctions as sfn
from constructs import Construct
from aje_cdk_libs.builders.resource_builder import ResourceBuilder
from aje_cdk_libs.builders.name_builder import NameBuilder
from aje_cdk_libs.constants.services import Services
from aje_cdk_libs.models.configs import GlueJobConfig, StepFunctionConfig
from constants.paths import Paths
import csv
import json

class CdkDatalakeIngestBigmagicGroupStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, project_config, process_id, src_db_name, base_stack_outputs, shared_table_info=None, shared_job_registry=None, **kwargs):
        super().__init__(scope, construct_id, **kwargs)
        self.PROJECT_CONFIG = project_config
        self.process_id = process_id
        self.src_db_name = src_db_name
        self.base_stack_outputs = base_stack_outputs
        self.shared_table_info = shared_table_info or {}
        self.shared_job_registry = shared_job_registry or {}
        self.created_jobs = {}
        self.builder = ResourceBuilder(self, self.PROJECT_CONFIG)
        self.name_builder = NameBuilder(self.PROJECT_CONFIG)
        self.Paths = Paths(self.PROJECT_CONFIG.app_config)

        
        self.glue_jobs = []
        self.job_name_registry = []  # Store actual job names for Step Function creation
        self.columns = self._read_columns_csv()
        self._import_shared_resources()
        self._create_deduplicated_glue_jobs()
        self._create_step_function()

    def _import_shared_resources(self):
        from aws_cdk import aws_s3 as s3, aws_dynamodb as dynamodb, aws_sns as sns, aws_iam as iam, aws_stepfunctions as sfn, aws_glue as glue
        
        self.s3_artifacts_bucket = s3.Bucket.from_bucket_name(
            self, "ArtifactsBucket", self.base_stack_outputs["ArtifactsBucketName"])
        self.s3_raw_bucket = s3.Bucket.from_bucket_name(
            self, "RawBucket", self.base_stack_outputs["RawBucketName"])
        self.s3_stage_bucket = s3.Bucket.from_bucket_name(
            self, "StageBucket", self.base_stack_outputs["StageBucketName"])
        self.s3_landing_bucket = s3.Bucket.from_bucket_name(
            self, "LandingBucket", self.base_stack_outputs["LandingBucketName"])
        self.s3_analytics_bucket = s3.Bucket.from_bucket_name(
            self, "AnalyticsBucket", self.base_stack_outputs["AnalyticsBucketName"])
        
        self.dynamo_logs_table = dynamodb.Table.from_table_name(
            self, "LogsTable", self.base_stack_outputs["DynamoLogsTableName"])
        
        self.sns_failed_topic = sns.Topic.from_topic_arn(
            self, "FailedTopic", self.base_stack_outputs["SnsFailedTopicArn"])
        self.sns_success_topic = sns.Topic.from_topic_arn(
            self, "SuccessTopic", self.base_stack_outputs["SnsSuccessTopicArn"])
        
        self.role_extract = iam.Role.from_role_arn(
            self, "ExtractRole", self.base_stack_outputs["RoleExtractArn"])
        self.role_light_transform = iam.Role.from_role_arn(
            self, "LightTransformRole", self.base_stack_outputs["RoleLightTransformArn"])
        self.role_crawler = iam.Role.from_role_arn(
            self, "CrawlerRole", self.base_stack_outputs["RoleCrawlerArn"])
        self.role_step_function = iam.Role.from_role_arn(
            self, "StepFunctionRole", self.base_stack_outputs["RoleStepFunctionArn"])
        
        if "GlueConnectionName" in self.base_stack_outputs:
            self.glue_connection_name = self.base_stack_outputs["GlueConnectionName"]
        else:
            self.glue_connection_name = None
        
        crawler_job_key = f"CrawlerJob{self.src_db_name}Name"
        if crawler_job_key in self.base_stack_outputs:
            self.crawler_job_name = self.base_stack_outputs[crawler_job_key]
        else:
            self.crawler_job_name = None
            
        crawler_key = f"Crawler{self.src_db_name}Name"
        if crawler_key in self.base_stack_outputs:
            self.crawler_name = self.base_stack_outputs[crawler_key]
            
        self.base_step_function = sfn.StateMachine.from_state_machine_arn(
            self, "BaseStepFunction", self.base_stack_outputs["BaseStepFunctionArn"])

    def _create_deduplicated_glue_jobs(self):
        tables = self._read_tables_csv()
        credentials = self._read_credentials_csv()
        
        if not tables:
            return
            
        if not credentials:
            return
        
        seen = set()
        self.glue_jobs = []
        self.job_name_registry = []  # Store actual job names for Step Function creation
        
        for row in tables:
            key = (row['SOURCE_SCHEMA'], row['SOURCE_TABLE'], self.src_db_name)
            if key in seen:
                continue
            seen.add(key)
            
            logical_name = row['STAGE_TABLE_NAME']
            process_ids = [int(pid.strip()) for pid in row['PROCESS_ID'].split(',')]
            is_shared = len(process_ids) > 1
            
            # Determine if this stack should create or reference the jobs
            if is_shared:
                is_primary_for_table = self.shared_table_info.get(logical_name, False)
                should_create_jobs = is_primary_for_table
            else:
                should_create_jobs = True
            
            if should_create_jobs:
                # Create the actual jobs in this stack
                job_info = self._create_jobs_for_table(row, logical_name, credentials)
                # Track created jobs for the registry
                self.created_jobs[logical_name] = job_info
                # Add to registry for Step Function creation
                self.job_name_registry.append({
                    'extract_job_name': job_info['extract_job_name'],
                    'light_job_name': job_info['light_job_name'],
                    'table_name': logical_name
                })
            else:
                # Reference jobs from the registry (created in another stack)
                job_info = self._reference_jobs_from_registry(logical_name)
                if job_info:
                    # Add referenced jobs to our registry too
                    self.job_name_registry.append({
                        'extract_job_name': job_info['extract_job_name'],
                        'light_job_name': job_info['light_job_name'],
                        'table_name': logical_name
                    })

    def _create_jobs_for_table(self, row, logical_name, credentials):
        import aws_cdk.aws_glue_alpha as glue
        import json
        from aje_cdk_libs.models.configs import GlueJobConfig
        
        table_columns = [col for col in self.columns if col['TABLE_NAME'].upper() == logical_name.upper()]
        
        table_config = {
            'SOURCE_SCHEMA': row.get('SOURCE_SCHEMA', ''),
            'SOURCE_TABLE': row.get('SOURCE_TABLE', ''),
            'STAGE_TABLE_NAME': row.get('STAGE_TABLE_NAME', ''),
            'LOAD_TYPE': row.get('LOAD_TYPE', 'full'),
            'ID_COLUMN': row.get('ID_COLUMN', ''),
            'FILTER_COLUMN': row.get('FILTER_COLUMN', ''),
            'FILTER_EXP': row.get('FILTER_EXP', ''),
            'SOURCE_TABLE_TYPE': row.get('SOURCE_TABLE_TYPE', 'm'),
            'DELAY_INCREMENTAL_INI': row.get('DELAY_INCREMENTAL_INI', '-2'),
            'COLUMNS': row.get('COLUMNS', '')
        }
        
        db_config = {
            'SRC_SERVER_NAME': credentials.get('SRC_SERVER_NAME', ''),
            'SRC_DB_NAME': credentials.get('SRC_DB_NAME', ''),
            'SRC_DB_USERNAME': credentials.get('SRC_DB_USERNAME', ''),
            'SRC_DB_SECRET': credentials.get('SRC_DB_SECRET', ''),
            'BD_TYPE': credentials.get('BD_TYPE', 'mssql'),
            'DB_PORT_NUMBER': credentials.get('DB_PORT_NUMBER', '1433')
        }
        
        # Check for Glue connections - prioritize base stack connection over CSV connection
        connections = []
        
        # First, check if base stack has provided the extract connection
        # Construct the expected output key based on the datasource from config
        datasource = self.PROJECT_CONFIG.app_config.get('datasource', 'bigmagic').lower()
        connection_logical_name = f"{datasource}-extract-connection"
        clean_name = connection_logical_name.replace('-', '').replace('_', '').title()
        extract_connection_key = f"GlueConnection{clean_name}Name"
        
        if extract_connection_key in self.base_stack_outputs:
            # Import the connection by name to get an IConnection object
            import aws_cdk.aws_glue_alpha as glue
            connection_name = self.base_stack_outputs[extract_connection_key]
            connection_obj = glue.Connection.from_connection_name(
                self, f"ImportedExtractConnection{self.src_db_name}{logical_name}",
                connection_name
            )
            connections = [connection_obj]
        
        # Fallback: check if a connection name is provided in credentials CSV (legacy support)
        elif credentials.get('GLUE_CONNECTION_NAME'):
            import aws_cdk.aws_glue_alpha as glue
            connection_name = credentials.get('GLUE_CONNECTION_NAME')
            connection_obj = glue.Connection.from_connection_name(
                self, f"ImportedConnectionCSV-{logical_name}",
                connection_name
            )
            connections = [connection_obj]
        
        # Prepare columns config - convert to the format expected by scripts
        columns_config = []
        for col in table_columns:
            columns_config.append({
                'COLUMN_NAME': col.get('COLUMN_NAME', ''),
                'COLUMN_ID': col.get('COLUMN_ID', ''),
                'NEW_DATA_TYPE': col.get('NEW_DATA_TYPE', 'string'),
                'TRANSFORMATION': col.get('TRANSFORMATION', ''),
                'IS_PARTITION': col.get('IS_PARTITION', '').lower() == 'true',
                'IS_ID': col.get('IS_ID', '').lower() == 'true',
                'IS_ORDER_BY': col.get('IS_ORDER_BY', '').lower() == 'true',
                'IS_FILTER_DATE': col.get('IS_FILTER_DATE', '').lower() == 'true'
            })
        
        # Extract job - Include datasource and database name to avoid conflicts between databases
        extract_job_descriptive_name = f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_extract_{logical_name.lower()}_{self.src_db_name.lower()}"
        # Generate the actual job name that will be created by CDK libs
        extract_job_name = self.name_builder.build(Services.GLUE_JOB, extract_job_descriptive_name)
        extract_tags = self._create_job_tags('Extract')
        
        extract_job_config = GlueJobConfig(
            job_name=extract_job_descriptive_name,  # Pass only the descriptive part to ResourceBuilder
            executable=glue.JobExecutable.python_shell(
                glue_version=glue.GlueVersion.V1_0,
                python_version=glue.PythonVersion.THREE_NINE,
                script=glue.Code.from_bucket(
                    self.s3_artifacts_bucket,
                    f"{self.Paths.AWS_ARTIFACTS_GLUE_CODE_RAW}/extract_data.py"
                )
            ),
            default_arguments={
                '--S3_RAW_PREFIX': f"s3://{self.s3_raw_bucket.bucket_name}/",
                '--ARN_TOPIC_FAILED': self.sns_failed_topic.topic_arn,
                '--ARN_TOPIC_SUCCESS': self.sns_success_topic.topic_arn,
                '--PROJECT_NAME': self.PROJECT_CONFIG.project_name,
                '--TEAM': self.PROJECT_CONFIG.app_config['team'],
                '--DATA_SOURCE': self.PROJECT_CONFIG.app_config['datasource'],
                '--ENVIRONMENT': self.PROJECT_CONFIG.environment.value.upper(),
                '--REGION': self.PROJECT_CONFIG.region_name,
                '--TABLE_NAME': logical_name,
                '--DYNAMO_LOGS_TABLE': self.dynamo_logs_table.table_name,
                '--SRC_DB_NAME': self.src_db_name,
                # Include aje_libs for dependencies
                '--extra-py-files': f"s3://{self.s3_artifacts_bucket.bucket_name}/athenea/bigmagic/aws-glue/layer/aje_libs.zip",
                '--additional-python-modules': f"aws-lambda-powertools, pymssql",
                'library-set': 'analytics',
                # Configuration parameters - pass CSV paths instead of large JSON to avoid template size limits
                '--TABLES_CSV_S3': f"s3://{self.s3_artifacts_bucket.bucket_name}/athenea/bigmagic/configuration/csv/tables.csv",
                '--CREDENTIALS_CSV_S3': f"s3://{self.s3_artifacts_bucket.bucket_name}/athenea/bigmagic/configuration/csv/credentials.csv",
                '--COLUMNS_CSV_S3': f"s3://{self.s3_artifacts_bucket.bucket_name}/athenea/bigmagic/configuration/csv/columns.csv"
            },
            continuous_logging=glue.ContinuousLoggingProps(enabled=True),
            timeout=Duration.minutes(60),
            max_concurrent_runs=20,
            role=self.role_extract,
            connections=connections if connections else None,
            tags=extract_tags
        )
        extract_job = self.builder.build_glue_job(extract_job_config)
        self.glue_jobs.append(extract_job)
        
        # Light transform job - Include datasource and database name to avoid conflicts between databases
        light_job_descriptive_name = f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_light_transform_{logical_name.lower()}_{self.src_db_name.lower()}"
        # Generate the actual job name that will be created by CDK libs
        light_job_name = self.name_builder.build(Services.GLUE_JOB, light_job_descriptive_name)
        light_transform_tags = self._create_job_tags('LightTransform')
        
        light_job_config = GlueJobConfig(
            job_name=light_job_descriptive_name,  # Pass only the descriptive part to ResourceBuilder
            executable=glue.JobExecutable.python_etl(
                glue_version=glue.GlueVersion.V4_0,
                python_version=glue.PythonVersion.THREE,
                script=glue.Code.from_bucket(
                    self.s3_artifacts_bucket,
                    f"{self.Paths.AWS_ARTIFACTS_GLUE_CODE_STAGE}/light_transform.py"
                )
            ),
            default_arguments={
                '--S3_RAW_PREFIX': f"s3://{self.s3_raw_bucket.bucket_name}/",
                '--S3_STAGE_PREFIX': f"s3://{self.s3_stage_bucket.bucket_name}/",
                '--ARN_TOPIC_FAILED': self.sns_failed_topic.topic_arn,
                '--ARN_TOPIC_SUCCESS': self.sns_success_topic.topic_arn,
                '--PROJECT_NAME': self.PROJECT_CONFIG.project_name,
                '--TEAM': self.PROJECT_CONFIG.app_config['team'],
                '--DATA_SOURCE': self.PROJECT_CONFIG.app_config['datasource'],
                '--ENVIRONMENT': self.PROJECT_CONFIG.environment.value.upper(),
                '--REGION': self.PROJECT_CONFIG.region_name,
                '--TABLE_NAME': logical_name,
                '--DYNAMO_LOGS_TABLE': self.dynamo_logs_table.table_name,
                '--SRC_DB_NAME': self.src_db_name,
                # Include aje_libs for dependencies
                '--extra-py-files': f"s3://{self.s3_artifacts_bucket.bucket_name}/athenea/bigmagic/aws-glue/layer/aje_libs.zip",
                # Enable Delta Lake support using the proper Glue 4.0 parameter
                '--datalake-formats': 'delta',
                # Configuration parameters - pass CSV paths instead of large JSON to avoid template size limits
                '--TABLES_CSV_S3': f"s3://{self.s3_artifacts_bucket.bucket_name}/athenea/bigmagic/configuration/csv/tables.csv",
                '--CREDENTIALS_CSV_S3': f"s3://{self.s3_artifacts_bucket.bucket_name}/athenea/bigmagic/configuration/csv/credentials.csv",
                '--COLUMNS_CSV_S3': f"s3://{self.s3_artifacts_bucket.bucket_name}/athenea/bigmagic/configuration/csv/columns.csv"
            },
            worker_type=glue.WorkerType.G_1_X,
            worker_count=2,
            continuous_logging=glue.ContinuousLoggingProps(enabled=True),
            timeout=Duration.hours(3),
            max_concurrent_runs=20,
            role=self.role_light_transform,
            tags=light_transform_tags
        )
        light_job = self.builder.build_glue_job(light_job_config)
        self.glue_jobs.append(light_job)
        
        # Return job information for the registry - use the actual CDK-generated names
        return {
            'extract_job_name': extract_job_name,  # Use the full CDK-generated name
            'light_job_name': light_job_name,      # Use the full CDK-generated name
            'extract_job': extract_job,
            'light_job': light_job
        }

    def _reference_jobs_from_registry(self, logical_name):
        """Reference existing jobs from shared registry"""
        registry_key = (logical_name, self.src_db_name)
        if registry_key in self.shared_job_registry:
            job_info = self.shared_job_registry[registry_key]
            # Create job references for cross-stack orchestration
            class JobReference:
                def __init__(self, job_name):
                    self.job_name = job_name
                    # Add type flag to identify this as a JobReference
                    self.is_job_reference = True
            
            extract_job_ref = JobReference(job_info['extract_job_name'])
            light_job_ref = JobReference(job_info['light_job_name'])
            
            self.glue_jobs.extend([extract_job_ref, light_job_ref])
            return job_info  # Return the job info for registry tracking
        else:
            # Missing registry entry, skip this table
            return None

    def _create_step_function(self):
        """Create orchestration Step Function for all group jobs"""
        import aws_cdk.aws_stepfunctions as sfn
        import aws_cdk.aws_stepfunctions_tasks as tasks
        
        # Extract job names from our registry (not CDK objects)
        job_configs = []
        extract_jobs = []
        light_transform_jobs = []
        
        for job_info in self.job_name_registry:
            # Process extract job
            extract_job_name = job_info['extract_job_name']
            light_job_name = job_info['light_job_name']
            table_name = job_info['table_name']
            
            extract_jobs.append(extract_job_name)
            light_transform_jobs.append(light_job_name)
            
            # Add extract job config
            job_configs.append({
                "job_name": extract_job_name,
                "table_name": table_name,
                "job_type": "extract"
            })
            
            # Add light transform job config
            job_configs.append({
                "job_name": light_job_name,
                "table_name": table_name,
                "job_type": "light_transform"
            })
        
        # Create a Step Function that uses the base Step Function from the base stack
        if job_configs:
            # Split jobs by type
            extract_job_configs = [job for job in job_configs if job["job_type"] == "extract"]
            transform_job_configs = [job for job in job_configs if job["job_type"] == "light_transform"]
            
            # Create a Pass state to prepare extract job configs
            prepare_extract_jobs = sfn.Pass(
                self, "PrepareExtractJobs",
                parameters={
                    "jobs": extract_job_configs,
                    "process_id": str(self.process_id),
                    "database": self.src_db_name,
                    "execution_start.$": "$$.Execution.StartTime",
                    "job_type": "extract"
                },
                result_path="$.extract_job_configs"
            )
            
            # Create a Map state for extract jobs with concurrency control
            extract_map_state = sfn.Map(
                self, "ProcessExtractJobs",
                max_concurrency=15,
                items_path="$.extract_job_configs.jobs",
                result_path="$.extract_results"
            )
            
            # Create a Pass state to prepare transform job configs
            prepare_transform_jobs = sfn.Pass(
                self, "PrepareTransformJobs",
                parameters={
                    "jobs": transform_job_configs,
                    "process_id": str(self.process_id),
                    "database": self.src_db_name,
                    "execution_start.$": "$$.Execution.StartTime",
                    "job_type": "transform"
                },
                result_path="$.transform_job_configs"
            )
            
            # Create a Map state for transform jobs with concurrency control
            transform_map_state = sfn.Map(
                self, "ProcessTransformJobs",
                max_concurrency=15,
                items_path="$.transform_job_configs.jobs",
                result_path="$.transform_results"
            )
            
            # Create a task to invoke the base Step Function for extract jobs
            invoke_extract_job = tasks.StepFunctionsStartExecution(
                self, "InvokeExtractJob",
                state_machine=self.base_step_function,
                integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                input=sfn.TaskInput.from_object({
                    "job_name": sfn.JsonPath.string_at("$.job_name"),
                    "job_arguments": {
                        "--TABLE_NAME": sfn.JsonPath.string_at("$.table_name"),
                        "--PROCESS_ID": str(self.process_id),  # Use the process_id directly from stack construction
                        "--SRC_DB_NAME": self.src_db_name  # Use the database name directly from stack construction
                    }
                }),
                result_path="$.execution_result"
            )
            
            # Add retry for extract job execution failures
            invoke_extract_job.add_retry(
                max_attempts=2,
                interval=Duration.seconds(10),
                backoff_rate=1.5,
                errors=["States.TaskFailed", "Lambda.ServiceException", "Lambda.AWSLambdaException"]
            )
            
            # Create a separate task to invoke the base Step Function for transform jobs
            invoke_transform_job = tasks.StepFunctionsStartExecution(
                self, "InvokeTransformJob",
                state_machine=self.base_step_function,
                integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                input=sfn.TaskInput.from_object({
                    "job_name": sfn.JsonPath.string_at("$.job_name"),
                    "job_arguments": {
                        "--TABLE_NAME": sfn.JsonPath.string_at("$.table_name"),
                        "--PROCESS_ID": str(self.process_id),  # Use the process_id directly from stack construction
                        "--SRC_DB_NAME": self.src_db_name  # Use the database name directly from stack construction
                    }
                }),
                result_path="$.execution_result"
            )
            
            # Add retry for transform job execution failures
            invoke_transform_job.add_retry(
                max_attempts=2,
                interval=Duration.seconds(10),
                backoff_rate=1.5,
                errors=["States.TaskFailed", "Lambda.ServiceException", "Lambda.AWSLambdaException"]
            )
            
            # Set the Map states' iterator to their respective invoke tasks
            extract_map_state.iterator(invoke_extract_job)
            transform_map_state.iterator(invoke_transform_job)
            
            # Create a task to invoke the crawler job after transform jobs
            if self.crawler_job_name:
                # Create a Pass state to prepare crawler job parameters
                prepare_crawler_job = sfn.Pass(
                    self, "PrepareCrawlerJob",
                    parameters={
                        "job_name": self.crawler_job_name,
                        "process_id": str(self.process_id),
                        "database": self.src_db_name,
                        "execution_start.$": "$$.Execution.StartTime"
                    },
                    result_path="$.crawler_job_config"
                )
                
                # Create a task to invoke the base Step Function for the crawler job
                invoke_crawler_job = tasks.StepFunctionsStartExecution(
                    self, "InvokeCrawlerJob",
                    state_machine=self.base_step_function,
                    integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                    input=sfn.TaskInput.from_object({
                        "job_name": sfn.JsonPath.string_at("$.crawler_job_config.job_name"),
                        "job_arguments": {
                            "--PROCESS_ID": sfn.JsonPath.string_at("$.crawler_job_config.process_id"),
                            "--SRC_DB_NAME": sfn.JsonPath.string_at("$.crawler_job_config.database")
                        }
                    }),
                    result_path="$.crawler_execution_result"
                )
                
                # Add retry for crawler job execution failures
                invoke_crawler_job.add_retry(
                    max_attempts=2,
                    interval=Duration.seconds(10),
                    backoff_rate=1.5,
                    errors=["States.TaskFailed", "Lambda.ServiceException", "Lambda.AWSLambdaException"]
                )
                
                # Add crawler job to the workflow chain
                crawler_workflow = prepare_crawler_job.next(invoke_crawler_job)
            else:
                # Create a pass state to skip the crawler if not available
                crawler_workflow = sfn.Pass(
                    self, "SkipCrawler",
                    result_path="$.skip_crawler"
                )
            
            # Workflow ends after crawler (catalog job removed)
            definition = prepare_extract_jobs.next(extract_map_state).next(prepare_transform_jobs).next(transform_map_state).next(crawler_workflow)
            
            # Create error handler states
            processing_failed = sfn.Fail(
                self, "ProcessingFailed",
                cause="One or more Glue jobs failed",
                error="MultipleJobFailures"
            )
            
            handle_map_failures = sfn.Pass(
                self, "HandleMapFailures",
                parameters={
                    "error_details.$": "$",
                    "process_id": str(self.process_id),
                    "database": self.src_db_name,
                    "timestamp.$": "$$.State.EnteredTime",
                    "error_summary": "Map state execution failed - check error_details for complete information"
                },
                result_path="$.map_error_details"
            ).next(processing_failed)
            
            # Add error handlers to the Map states
            extract_map_state.add_catch(handle_map_failures, errors=["States.ALL"], result_path="$.extract_error")
            transform_map_state.add_catch(handle_map_failures, errors=["States.ALL"], result_path="$.transform_error")
        else:
            definition = sfn.Pass(self, "NoTablesToProcess")
        
        # Create the Step Function that uses the base Step Function
        sf_name = f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_orchestrate_extract_{self.src_db_name.lower()}_{self.process_id}"
        
        # Use aje_cdk_libs approach for consistency with base stack
        from aje_cdk_libs.models.configs import StepFunctionConfig
        
        step_function_tags = self._create_job_tags('Orchestration')
        
        sf_config = StepFunctionConfig(
            name=sf_name,
            definition=definition,
            timeout=Duration.hours(4),  # Extend timeout to handle multiple jobs
            role=self.role_step_function,
            tags=step_function_tags
        )
        
        self.step_function = self.builder.build_step_function(sf_config)

    def _read_tables_csv(self):
        """Load tables for current process_id with STATUS = 'a'"""
        tables = []
        
        with open('artifacts/configuration/csv/tables.csv', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            for row in reader:
                if not (row['SOURCE_SCHEMA'] and row['SOURCE_TABLE']):
                    continue
                
                # Filter by STATUS = 'a' only
                if row.get('STATUS', '').lower() != 'a':
                    continue
                    
                # Handle multi/single process tables
                if ',' in row['PROCESS_ID']:
                    process_ids = [pid.strip() for pid in row['PROCESS_ID'].split(',')]
                    if str(self.process_id) in process_ids:
                        tables.append(row)
                else:
                    # Single process table
                    if row['PROCESS_ID'] == str(self.process_id):
                        tables.append(row)
                        
        return tables

    def _read_credentials_csv(self):
        """Load credentials for current src_db_name and environment"""
        creds = None
        current_env = self.PROJECT_CONFIG.environment.value.upper()  # Get current environment (DEV/PROD)
        
        with open('artifacts/configuration/csv/credentials.csv', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            for row in reader:
                # Match both SRC_DB_NAME and ENV
                if (row['SRC_DB_NAME'] == self.src_db_name and 
                    row.get('ENV', '').upper() == current_env):
                    creds = row
                    break
            
        return creds

    def _read_columns_csv(self):
        """Load columns for tables in current group"""
        # Get logical table names
        tables = self._read_tables_csv()
        logical_names = set(row['STAGE_TABLE_NAME'].upper() for row in tables if row.get('STAGE_TABLE_NAME'))
        columns = []
        with open('artifacts/configuration/csv/columns.csv', newline='', encoding='latin-1') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            for row in reader:
                if row.get('TABLE_NAME', '').upper() in logical_names:
                    columns.append(row)
        return columns

    def _create_job_tags(self, job_type):
        """Generate resource tags"""
        # Get instance information from credentials
        credentials = self._read_credentials_csv()
        instance = credentials.get('INSTANCE', '') if credentials else ''
        
        tags = {
            'DataSource': self.PROJECT_CONFIG.app_config.get('datasource', ''),
            'Endpoint': self.src_db_name,
            'Instance': instance,
            'Process': 'Ingest',
            'SubProcess': job_type
        }
        return {k: v for k, v in tags.items() if v}

    def _upload_configurations_to_s3(self, logical_name, table_config, db_config, columns_config):
        """Store configuration parameters as S3 paths to avoid CloudFormation template size limits"""
        # Instead of uploading files, we'll just pass the S3 paths and let the Glue jobs
        # read the configuration from the CSV files directly. This avoids both:
        # 1. Large CloudFormation templates
        # 2. Complex S3 deployment during stack creation
        
        # The Glue jobs will read configurations directly from CSV files using:
        # - Table name for filtering
        # - Database name for credentials lookup
        # - This matches the existing CSV structure
        pass