import csv
import os
import json
from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_sns as sns,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_secretsmanager as secretsmanager,
    CfnOutput,
    Duration,
    Tags
)
import aws_cdk.aws_glue_alpha as glue
import aws_cdk.aws_s3_deployment as s3_deployment
from constructs import Construct

from aje_cdk_libs.builders.name_builder import NameBuilder
from aje_cdk_libs.builders.resource_builder import ResourceBuilder
from aje_cdk_libs.constants.policies import PolicyUtils
from aje_cdk_libs.constants.services import Services
from aje_cdk_libs.models.configs import GlueJobConfig, GlueRoleConfig, S3DeploymentConfig, SecretConfig, StepFunctionConfig, GlueConnectionConfig, GlueJdbcConnectionConfig,LambdaConfig
from constants.paths import Paths        
        

class CdkDatalakeIngestBigmagicStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, project_config, process_config=None, **kwargs):
        super().__init__(scope, construct_id, **kwargs)
        self.PROJECT_CONFIG = project_config
        self.builder = ResourceBuilder(self, self.PROJECT_CONFIG)
        self.name_builder = NameBuilder(self.PROJECT_CONFIG)
        self.Paths = Paths(self.PROJECT_CONFIG.app_config)

        # Initialize resource dictionaries
        self.glue_connections = {}
        self.secrets = {}
        self.relation_process = {}

        self._get_s3_buckets()
        self._get_dynamodb_tables()
        self._get_sns_topics()
        self._create_secrets()  # Create secrets for database credentials
        self._create_iam_roles()
        self._create_glue_connections()  # Create Glue connections for extract jobs
        self._deploy_scripts_to_s3()  # Deploy scripts to S3 before creating jobs
        self._create_outputs()
        self._create_crawler_glue_jobs()  # Create crawler jobs for each database
        self._create_base_step_function()  # Create a reusable base Step Function
        self._create_legacy_lambda_function()  # Create domain lambda function
        self._create_domain_lambda_function()  # Create domain lambda function


    def _get_s3_buckets(self):
        self.s3_artifacts_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["artifacts"])
        self.s3_landing_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["landing"])
        self.s3_raw_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["raw"])
        self.s3_stage_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["stage"])
        self.s3_analytics_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["analytics"])
        self.s3_external_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["external"])

    def _get_dynamodb_tables(self):
        self.dynamodb_logs_table = self.builder.import_dynamodb_table(self.PROJECT_CONFIG.app_config["dynamodb_tables"]["logs"])

    def _get_sns_topics(self):
        self.sns_failed_topic = self.builder.import_sns_topic(self.PROJECT_CONFIG.app_config["topic_notifications"]["failed"])
        self.sns_success_topic = self.builder.import_sns_topic(self.PROJECT_CONFIG.app_config["topic_notifications"]["success"])

    def _create_iam_roles(self):
        resources = self._get_resource_arns()

        extract_tags = self._create_job_tags("Extract")
        light_transform_tags = self._create_job_tags("LightTransform")
        crawler_tags = self._create_job_tags("Crawler")
        step_function_tags = self._create_job_tags("StepFunction")
        lambda_tags = self._create_job_tags('Lambda')

        self.role_extract = self.builder.build_extract_role(f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_extract", resources, extract_tags)

        self.role_light_transform = self.builder.build_light_transform_role(f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_light_transform", resources, light_transform_tags)

        self.role_crawler = self.builder.build_crawler_role(f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_crawler", resources, crawler_tags)

        self.role_crawler.add_to_principal_policy(iam.PolicyStatement(effect=iam.Effect.ALLOW, actions=["iam:PassRole"], resources=[self.role_crawler.role_arn]))

        self.role_step_function = self.builder.build_step_function_role(f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_step_function", resources, step_function_tags)

        self.role_lambda = self._build_lambda_role(f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_lambda",resources,lambda_tags)        

    def _create_outputs(self):
        CfnOutput(self, "ArtifactsBucketName", value=self.s3_artifacts_bucket.bucket_name)
        CfnOutput(self, "RawBucketName", value=self.s3_raw_bucket.bucket_name)
        CfnOutput(self, "StageBucketName", value=self.s3_stage_bucket.bucket_name)
        CfnOutput(self, "LandingBucketName", value=self.s3_landing_bucket.bucket_name)
        CfnOutput(self, "DynamoLogsTableName", value=self.dynamodb_logs_table.table_name)
        CfnOutput(self, "SnsFailedTopicArn", value=self.sns_failed_topic.topic_arn)
        CfnOutput(self, "SnsSuccessTopicArn", value=self.sns_success_topic.topic_arn)
        CfnOutput(self, "RoleExtractArn", value=self.role_extract.role_arn)
        CfnOutput(self, "RoleLightTransformArn", value=self.role_light_transform.role_arn)
        CfnOutput(self, "RoleCrawlerArn", value=self.role_crawler.role_arn)
        CfnOutput(self, "RoleStepFunctionArn", value=self.role_step_function.role_arn)


        # Output secret name/ARN for cross-stack reference (single secret per stack)
        if "main_secret" in self.secrets:
            secret = self.secrets["main_secret"]
            CfnOutput(self, "SecretName", value=secret.secret_name)
            CfnOutput(self, "SecretArn", value=secret.secret_arn)

    def _create_catalog_job(self, endpoint_name):
        from aws_cdk import Duration
        import aws_cdk.aws_glue_alpha as glue
        from aje_cdk_libs.models.configs import GlueJobConfig

        job_name = f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_catalog_{endpoint_name.lower()}"
        catalog_tags = self._create_job_tags("Catalog", endpoint_name)

        catalog_job_config = GlueJobConfig(
            job_name=job_name,
            executable=glue.JobExecutable.python_shell(
                glue_version=glue.GlueVersion.V1_0, python_version=glue.PythonVersion.THREE_NINE, script=glue.Code.from_bucket(self.s3_artifacts_bucket, f"{self.Paths.AWS_ARTIFACTS_GLUE_CODE_STAGE}/crawler_catalog.py")
            ),
            default_arguments={
                "--S3_STAGE_BUCKET": self.s3_stage_bucket.bucket_name,
                "--DYNAMO_LOGS_TABLE": self.dynamodb_logs_table.table_name,
                "--TEAM": self.PROJECT_CONFIG.app_config["team"],
                "--DATA_SOURCE": self.PROJECT_CONFIG.app_config["datasource"],
                "--REGION": self.PROJECT_CONFIG.region_name,
                "--ENVIRONMENT": self.PROJECT_CONFIG.environment.value,
                "--ENDPOINT_NAME": endpoint_name,
            },
            continuous_logging=glue.ContinuousLoggingProps(enabled=True),
            timeout=Duration.minutes(30),
            max_concurrent_runs=10,
            role=self.role_crawler,  # Use crawler role for catalog operations
            tags=catalog_tags,
        )

        return self.builder.build_glue_job(catalog_job_config)

    def _create_crawler_glue_jobs(self):
        """Create crawler jobs for each endpoint"""
        # Read credentials to get endpoint list that match current environment
        endpoint_names = []
        with open(f"{self.Paths.LOCAL_ARTIFACTS_CONFIGURE_CSV}/credentials.csv", newline="", encoding="utf-8") as creds_file:
            creds_reader = csv.DictReader(creds_file, delimiter=";")
            for row in creds_reader:
                # Only include endpoints that match the current environment
                if row["ENV"].lower() == self.PROJECT_CONFIG.environment.value.lower():
                    endpoint_names.append(row["ENDPOINT_NAME"])
        # Create crawler jobs for each endpoint - both Raw and Stage
        self.crawler_jobs = {}

        for endpoint_name in endpoint_names:
            # Create job name for this endpoint
            job_name = f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_crawler_{endpoint_name.lower()}"
            crawler_tags = self._create_job_tags("Crawler", endpoint_name)

            # Create crawler configuration JSON for this specific endpoint
            crawler_config = {
                "endpoint_name": endpoint_name,
                "endpoint_names": [endpoint_name],  # This crawler handles only this endpoint
                "crawler_settings": {"table_prefix": "", "s3_path_prefix": f"s3://{self.s3_stage_bucket.bucket_name}/{endpoint_name}/", "database_prefix": f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_", "partition_detection": True},
            }

            crawler_job_config = GlueJobConfig(
                job_name=job_name,
                executable=glue.JobExecutable.python_shell(
                    glue_version=glue.GlueVersion.V1_0, python_version=glue.PythonVersion.THREE_NINE, script=glue.Code.from_bucket(self.s3_artifacts_bucket, f"{self.Paths.AWS_ARTIFACTS_GLUE_CODE_STAGE}/crawler_stage.py")
                ),
                default_arguments={
                    "--S3_STAGE_BUCKET": self.s3_stage_bucket.bucket_name,
                    "--DYNAMO_LOGS_TABLE": self.dynamodb_logs_table.table_name,
                    "--PROCESS_ID": "ALL",  # Can be overridden at runtime
                    "--ENDPOINT_NAME": endpoint_name,  # This crawler handles this specific database
                    "--TEAM": self.PROJECT_CONFIG.app_config["team"],
                    "--DATA_SOURCE": self.PROJECT_CONFIG.app_config["datasource"],
                    "--REGION": self.PROJECT_CONFIG.region_name,
                    "--ENVIRONMENT": self.PROJECT_CONFIG.environment.value.lower(),
                    # Include aje_libs for dependencies
                    "--extra-py-files": f"s3://{self.s3_artifacts_bucket.bucket_name}/{self.Paths.AWS_ARTIFACTS_GLUE_LAYER}/aje_libs.zip",
                    "--CRAWLER_CONFIG": json.dumps(crawler_config),
                    "--ARN_ROLE_CRAWLER": self.role_crawler.role_arn,
                },
                continuous_logging=glue.ContinuousLoggingProps(enabled=True),
                timeout=Duration.minutes(60),
                max_concurrent_runs=20,
                role=self.role_crawler,
                tags=crawler_tags,  # Add tags
            )

            crawler_job = self.builder.build_glue_job(crawler_job_config)
            self.crawler_jobs[endpoint_name] = crawler_job

            # Create output for this crawler job
            CfnOutput(self, f"CrawlerJob{endpoint_name}Name", value=crawler_job.job_name)

        # Create database-specific crawlers and catalog jobs
        self._create_glue_crawlers()

    def _create_glue_crawlers(self):
        """Create Glue crawlers and catalog jobs for each database"""
        import csv
        from aws_cdk import aws_glue as glue_cfn
        import json

        # Read credentials to get database list that match current environment
        endpoint_names = []
        with open(f"{self.Paths.LOCAL_ARTIFACTS_CONFIGURE_CSV}/credentials.csv", newline="", encoding="utf-8") as creds_file:
            creds_reader = csv.DictReader(creds_file, delimiter=";")
            for row in creds_reader:
                # Only include databases that match the current environment
                if row["ENV"].lower() == self.PROJECT_CONFIG.environment.value.lower():
                    endpoint_names.append(row["ENDPOINT_NAME"])

        # Create crawlers and catalog jobs for each database
        self.crawlers = {}
        self.catalog_jobs = {}

        for endpoint_name in endpoint_names:
            # Sanitize database name for use in resource names
            safe_endpoint_name = endpoint_name.replace("_", "-").lower()

            # 1. Create a Glue Data Catalog database for this source
            database_name = f"{self.PROJECT_CONFIG.app_config['team'].lower()}_{self.PROJECT_CONFIG.app_config['datasource'].lower()}_{safe_endpoint_name}"

            # 2. Create a Glue crawler to populate the database
            crawler_name = f"{self.PROJECT_CONFIG.app_config['team'].lower()}-{self.PROJECT_CONFIG.app_config['datasource'].lower()}-{safe_endpoint_name}-cw"
            crawler_tags = self._create_job_tags("Crawler", endpoint_name)

            # Define the S3 target path for this database
            s3_target_path = f"s3://{self.s3_stage_bucket.bucket_name}/{self.PROJECT_CONFIG.app_config['team'].lower()}/{self.PROJECT_CONFIG.app_config['datasource'].lower()}/{endpoint_name}/"

            # Create the crawler
            crawler = glue_cfn.CfnCrawler(
                self,
                f"GlueCrawler{endpoint_name}",
                name=crawler_name,
                role=self.role_crawler.role_arn,
                database_name=database_name,
                targets={"s3Targets": [{"path": s3_target_path}]},
                schema_change_policy={"updateBehavior": "UPDATE_IN_DATABASE", "deleteBehavior": "LOG"},
                configuration=json.dumps({"Version": 1.0, "CrawlerOutput": {"Partitions": {"AddOrUpdateBehavior": "InheritFromTable"}, "Tables": {"AddOrUpdateBehavior": "MergeNewColumns"}}}),
            )

            # Apply standard tags to crawler
            for key, value in crawler_tags.items():
                if value:  # Only add non-empty tags
                    crawler.add_property_override(f"Tags.{key}", value)

            # Store crawler reference
            self.crawlers[endpoint_name] = crawler
            CfnOutput(self, f"Crawler{self.PROJECT_CONFIG.app_config['team']}{self.PROJECT_CONFIG.app_config['datasource']}{endpoint_name}Name", value=crawler.name)

            # 3. Create a catalog job to run after transformation
            catalog_job = self._create_catalog_job(endpoint_name)
            self.catalog_jobs[endpoint_name] = catalog_job
            CfnOutput(self, f"CatalogJob{self.PROJECT_CONFIG.app_config['team']}{self.PROJECT_CONFIG.app_config['datasource']}{endpoint_name}Name", value=catalog_job.job_name)

    def _get_resource_arns(self):
        """Generate ARN maps for resource-specific permissions following least privilege principle"""
        resources = {
            "s3": [
                f"arn:aws:s3:::{self.s3_landing_bucket.bucket_name}",
                f"arn:aws:s3:::{self.s3_landing_bucket.bucket_name}/*",
                f"arn:aws:s3:::{self.s3_raw_bucket.bucket_name}",
                f"arn:aws:s3:::{self.s3_raw_bucket.bucket_name}/*",
                f"arn:aws:s3:::{self.s3_stage_bucket.bucket_name}",
                f"arn:aws:s3:::{self.s3_stage_bucket.bucket_name}/*",
                f"arn:aws:s3:::{self.s3_artifacts_bucket.bucket_name}",
                f"arn:aws:s3:::{self.s3_artifacts_bucket.bucket_name}/*",
            ],
            "dynamodb": [self.dynamodb_logs_table.table_arn],
            "sns": [self.sns_failed_topic.topic_arn, self.sns_success_topic.topic_arn],
            "glue": [
                f"arn:aws:glue:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:catalog",
                f"arn:aws:glue:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:database/*",
                f"arn:aws:glue:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:table/*",
                f"arn:aws:glue:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:crawler/*",
                f"arn:aws:glue:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:job/*",
            ],
            "states": [
                f"arn:aws:states:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:stateMachine:*"
                ],
            "lakeformation": [
                f"arn:aws:lakeformation:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:resource:*",
                f"arn:aws:lakeformation:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:catalog:{self.PROJECT_CONFIG.account_id}",
            ],
            "secret": [f"arn:aws:secretsmanager:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:secret:{self.secrets['main_secret'].secret_name}-*"],
            "lambda": [f"arn:aws:lambda:{self.PROJECT_CONFIG.region_name}:{self.PROJECT_CONFIG.account_id}:function:{self.PROJECT_CONFIG.project_name}-{self.PROJECT_CONFIG.environment.value.lower()}-datalake-invoke_*"],
            "iam": [f"arn:aws:iam::{self.PROJECT_CONFIG.account_id}:role/{self.PROJECT_CONFIG.project_name}-{self.PROJECT_CONFIG.environment.value.lower()}-datalake-*"],
        }
        return resources

    def _create_base_step_function(self):
        start_job = tasks.CallAwsService(
            self, "StartGlueJob", service="glue", action="startJobRun", parameters={"JobName": sfn.JsonPath.string_at("$.job_name"), "Arguments": sfn.JsonPath.object_at("$.job_arguments")}, iam_resources=["*"], result_path="$.job_run"
        )

        check_job_status = tasks.CallAwsService(
            self,
            "CheckGlueJobStatus",
            service="glue",
            action="getJobRun",
            parameters={"JobName": sfn.JsonPath.string_at("$.job_name"), "RunId": sfn.JsonPath.string_at("$.job_run.JobRunId")},
            iam_resources=["*"],  # Use wildcard to avoid policy size issues
            result_path="$.job_status",
        )

        job_status_choice = sfn.Choice(self, "IsJobComplete")

        wait_state = sfn.Wait(self, "WaitForJobCompletion", time=sfn.WaitTime.duration(Duration.seconds(30)))

        notify_success = tasks.CallAwsService(
            self,
            "NotifySuccess",
            service="sns",
            action="publish",
            parameters={"TopicArn": self.sns_success_topic.topic_arn, "Message": sfn.JsonPath.format("Glue job {} completed successfully", sfn.JsonPath.string_at("$.job_name")), "Subject": "Glue Job Success"},
            iam_resources=[self.sns_success_topic.topic_arn],
            result_path="$.notification_result",
        )

        job_failed = sfn.Fail(self, "JobFailed", cause="Glue Job Failed", error="JobExecutionFailed")

        job_succeeded = sfn.Succeed(self, "JobSucceeded")
        notify_job_failed = tasks.CallAwsService(
            self,
            "NotifyJobFailed",
            service="sns",
            action="publish",
            parameters={"TopicArn": self.sns_failed_topic.topic_arn, "Message": sfn.JsonPath.format("Glue job {} failed with state: FAILED", sfn.JsonPath.string_at("$.job_name")), "Subject": "Glue Job Failure"},
            iam_resources=[self.sns_failed_topic.topic_arn],
            result_path="$.notification_result",
        )

        notify_job_stopped = tasks.CallAwsService(
            self,
            "NotifyJobStopped",
            service="sns",
            action="publish",
            parameters={"TopicArn": self.sns_failed_topic.topic_arn, "Message": sfn.JsonPath.format("Glue job {} failed with state: STOPPED", sfn.JsonPath.string_at("$.job_name")), "Subject": "Glue Job Stopped"},
            iam_resources=[self.sns_failed_topic.topic_arn],
            result_path="$.notification_result",
        )

        notify_job_timeout = tasks.CallAwsService(
            self,
            "NotifyJobTimeout",
            service="sns",
            action="publish",
            parameters={"TopicArn": self.sns_failed_topic.topic_arn, "Message": sfn.JsonPath.format("Glue job {} failed with state: TIMEOUT", sfn.JsonPath.string_at("$.job_name")), "Subject": "Glue Job Timeout"},
            iam_resources=[self.sns_failed_topic.topic_arn],
            result_path="$.notification_result",
        )

        job_failed_chain = notify_job_failed.next(job_failed)
        job_stopped_chain = notify_job_stopped.next(job_failed)
        job_timeout_chain = notify_job_timeout.next(job_failed)
        check_job_status.next(job_status_choice)

        job_status_choice.when(sfn.Condition.string_equals("$.job_status.JobRun.JobRunState", "SUCCEEDED"), notify_success.next(job_succeeded)).when(sfn.Condition.string_equals("$.job_status.JobRun.JobRunState", "FAILED"), job_failed_chain).when(
            sfn.Condition.string_equals("$.job_status.JobRun.JobRunState", "STOPPED"), job_stopped_chain
        ).when(sfn.Condition.string_equals("$.job_status.JobRun.JobRunState", "TIMEOUT"), job_timeout_chain).otherwise(wait_state.next(check_job_status))

        definition = start_job.next(check_job_status)

        sf_name = f"workflow_base_{self.PROJECT_CONFIG.app_config['datasource'].lower()}"

        sf_tags = self._create_job_tags("BaseWorkflow")

        sf_config = StepFunctionConfig(name=sf_name, definition_body=sfn.DefinitionBody.from_chainable(definition), timeout=Duration.hours(12), role=self.role_step_function, tags=sf_tags)  # Long timeout for long-running jobs

        self.base_step_function = self.builder.build_step_function(sf_config)

        CfnOutput(self, "BaseStepFunctionArn", value=self.base_step_function.state_machine_arn, description="ARN of the base step function that executes Glue jobs")

    def _create_job_tags(self, job_type, endpoint=None):
        """Generate resource tags"""
        tags = {"DataSource": self.PROJECT_CONFIG.app_config.get("datasource", ""), "Process": "Ingest", "SubProcess": job_type}

        if endpoint:
            tags["Endpoint"] = endpoint
            # Get instance information from credentials for this endpoint
            instance = self._get_instance_for_endpoint(endpoint)
            if instance:
                tags["Instance"] = instance

        return {k: v for k, v in tags.items() if v}

    def _deploy_scripts_to_s3(self):
        """Deploy Glue code and CSV configuration files to S3 buckets"""
        # Deploy raw scripts
        resource_name = "raw"
        config = S3DeploymentConfig(
            f"BucketDeploymentJobsGlueCode{self.PROJECT_CONFIG.app_config['datasource'].lower()}{resource_name}",
            [s3_deployment.Source.asset(f"{self.Paths.LOCAL_ARTIFACTS_GLUE_CODE}/{resource_name}")],
            self.s3_artifacts_bucket,
            f"{self.Paths.AWS_ARTIFACTS_GLUE_CODE}/{resource_name}",
        )
        self.builder.deploy_s3_bucket(config)

        # Deploy stage scripts
        resource_name = "stage"
        config = S3DeploymentConfig(
            f"BucketDeploymentJobsGlueCode{self.PROJECT_CONFIG.app_config['datasource'].lower()}{resource_name}",
            [s3_deployment.Source.asset(f"{self.Paths.LOCAL_ARTIFACTS_GLUE_CODE}/{resource_name}")],
            self.s3_artifacts_bucket,
            f"{self.Paths.AWS_ARTIFACTS_GLUE_CODE}/{resource_name}",
        )
        self.builder.deploy_s3_bucket(config)

        # Deploy Layer glue job
        config = S3DeploymentConfig(
            f"BucketDeploymentLayerGlueJob{self.PROJECT_CONFIG.app_config['datasource'].lower()}", [s3_deployment.Source.asset(self.Paths.LOCAL_ARTIFACTS_GLUE_LAYER)], self.s3_artifacts_bucket, self.Paths.AWS_ARTIFACTS_GLUE_LAYER
        )
        self.builder.deploy_s3_bucket(config)

        # Deploy CSV configuration files
        config = S3DeploymentConfig(
            f"BucketDeploymentCSVConfigurations{self.PROJECT_CONFIG.app_config['datasource'].lower()}", [s3_deployment.Source.asset(self.Paths.LOCAL_ARTIFACTS_CONFIGURE_CSV)], self.s3_artifacts_bucket, self.Paths.AWS_ARTIFACTS_CONFIGURE_CSV
        )
        self.builder.deploy_s3_bucket(config)

    def _create_glue_connections(self):
        """Create Glue VPC connections for extract jobs that need database connectivity"""
        self.glue_connections = {}  # Initialize connections dictionary

        # Get VPC configuration from CDK context
        glue_config = self.PROJECT_CONFIG.app_config.get("glue_connection")

        if not glue_config:
            # No Glue connection configuration found, skip connection creation
            return

        # Extract configuration values
        vpc_id = glue_config.get("vpc_id")
        subnet_id = glue_config.get("subnet_id")
        security_group_ids = glue_config.get("security_group_ids", [])
        availability_zone = glue_config.get("availability_zone", "us-east-2a")

        # Validate required configuration
        if not all([vpc_id, subnet_id, security_group_ids]):
            # VPC connection configuration incomplete, skip connection creation
            return

        try:
            # Create connection name based on datasource - use logical name, not final name
            datasource = self.PROJECT_CONFIG.app_config["datasource"].lower()
            connection_logical_name = f"{datasource}-extract-connection"

            # Create configuration for the Glue connection
            config = GlueConnectionConfig(
                connection_name=connection_logical_name,
                vpc_id=vpc_id,
                subnet_id=subnet_id,
                security_group_id=security_group_ids[0],  # Use the first security group
                availability_zone=availability_zone,
                description=f"VPC connection for {datasource} extract jobs",
            )

            # Use the builder to create the connection
            connection = self.builder.build_glue_connection(config)

            # Store the connection for use by extract jobs (using logical name as key)
            self.glue_connections[connection_logical_name] = connection

        except Exception as e:
            # Note: Connection creation failed, extract jobs will run without VPC connection
            print(f"Warning: Failed to create glue connection '{glue_config}': {str(e)}")
            pass

    def _create_secrets(self):
        """Create a single AWS Secrets Manager secret for database credentials following extract_data.py naming convention"""

        # Initialize secrets dictionary
        self.secrets = {}

        # Get configuration values
        environment = self.PROJECT_CONFIG.environment.value.lower()  # dev/prod
        project_name = self.PROJECT_CONFIG.project_name.lower()
        team = self.PROJECT_CONFIG.app_config["team"].lower()
        data_source = self.PROJECT_CONFIG.app_config["datasource"].lower()

        # Create secret name following extract_data.py convention: {environment}/{project_name}/{team}/{data_source}
        secret_logical_name = f"{environment}/{project_name}/{team}/{data_source}"

        try:
            # Create a dummy secret value that will be manually updated later
            dummy_secret_value = {"username": "PLACEHOLDER_USERNAME", "password": "PLACEHOLDER_PASSWORD", "hostname": "PLACEHOLDER_HOSTNAME", "port": "PLACEHOLDER_PORT", "database": "PLACEHOLDER_DATABASE"}

            # Create secret configuration with string value (ResourceBuilder will convert to SecretValue)
            secret_config = SecretConfig(secret_name=secret_logical_name, secret_value=json.dumps(dummy_secret_value))

            # Create the secret using the builder
            secret = self.builder.build_secret(secret_config)

            # Store the secret for output reference (use a generic key since there's only one)
            self.secrets["main_secret"] = secret

        except Exception as e:
            # Log error but don't fail the stack deployment
            print(f"Warning: Failed to create secret '{secret_logical_name}': {str(e)}")
            pass

    def _get_instance_for_endpoint(self, endpoint: str) -> str:
        """Get instance information for a specific endpoint from credentials.csv"""
        try:
            current_env = self.PROJECT_CONFIG.environment.value.upper()
            with open(f"{self.Paths.LOCAL_ARTIFACTS_CONFIGURE_CSV}/credentials.csv", newline="", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile, delimiter=";")
                for row in reader:
                    # Match both SRC_DB_NAME and ENV
                    if row.get("ENDPOINT_NAME", "") == endpoint and row.get("ENV", "").upper() == current_env:
                        return row.get("INSTANCE", "")
            return ""
        except Exception as e:
            # If we can't read the file, just return empty string
            print(f"Warning: Could not read instance for endpoint {endpoint}: {e}")
            return ""


    def _create_legacy_lambda_function(self):
        """Create the lambda function that invokes the domain step function"""
        
        # Create domain lambda function
        lambda_tags = self._create_job_tags('DomainLambda')
        
        lambda_config = LambdaConfig(
            function_name=f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_invoke_legacy_function",
            handler="lambda_function.lambda_handler",
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE_RAW}/invoke_legacy_function",
            timeout=Duration.minutes(15),
            runtime=_lambda.Runtime("python3.13"),
            role=self.role_lambda,
            environment={
                'DATALAKE_AWS_REGION': Stack.of(self).region,
                'DATALAKE_AWS_ACCOUNT_ID': Stack.of(self).account,
                'DATALAKE_ENVIRONMENT': self.PROJECT_CONFIG.environment.value
            }
        )
        
        self.legacy_lambda_function = self.builder.build_lambda_function(lambda_config)
        
        # Add tags to the lambda function
        for key, value in lambda_tags.items():
            Tags.of(self.legacy_lambda_function).add(key, value)
        
        # Add lambda function ARN to outputs for reference by instance stacks
        CfnOutput(
            self, "LegacyLambdaFunctionArn", 
            value=self.legacy_lambda_function.function_arn,
            description="ARN of the lambda function that invokes legacy step function"
        )
        
        CfnOutput(
            self, "LegacyLambdaFunctionName", 
            value=self.legacy_lambda_function.function_name,
            description="Name of the lambda function that invokes legacy step function"
        )

    def _build_lambda_role(self, role_name, resources, tags):
        """Create a new IAM role for Lambda functions with appropriate permissions for S3, DynamoDB, and SNS"""

        # Format the role name using the name builder
        formatted_role_name = self.name_builder.build(Services.IAM_ROLE, role_name)

        inline_policies = {
            "AccessProviderS3": iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        actions=["s3:GetObject", "s3:ListBucket"], 
                        #resources=[f"arn:aws:s3:::{self.PROJECT_CONFIG.app_config['provider'].get('providerBucket')}", f"arn:aws:s3:::{self.PROJECT_CONFIG.app_config['provider'].get('providerBucket')}/*"]
                        resources=["*"]
                    )
                ]
            )
        }

        # Start with base Lambda role configuration
        role = iam.Role(
            self,
            formatted_role_name,
            role_name=formatted_role_name,
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
            inline_policies=inline_policies,
            description=f"Role for Domain Lambda functions",
        )

        # Define permission sets
        s3_permissions = PolicyUtils.S3_FULL
        dynamodb_permissions = PolicyUtils.DYNAMODB_FULL
        sns_permissions = PolicyUtils.SNS_FULL
        secrets_permissions = PolicyUtils.SECRET_MANAGER_READ
        states_permissions = ["states:StartExecution", "states:DescribeExecution", "states:GetExecutionHistory"]

        # Add resource-specific permissions
        if resources:
            # Add S3 permissions
            if "s3" in resources:
                role.add_to_policy(iam.PolicyStatement(effect=iam.Effect.ALLOW, actions=s3_permissions, resources=resources["s3"]))

            # Add DynamoDB permissions
            if "dynamodb" in resources:
                role.add_to_policy(iam.PolicyStatement(effect=iam.Effect.ALLOW, actions=dynamodb_permissions, resources=resources["dynamodb"]))

            # Add SNS permissions
            if "sns" in resources:
                role.add_to_policy(iam.PolicyStatement(effect=iam.Effect.ALLOW, actions=sns_permissions, resources=resources["sns"]))

            # Add Secrets Manager permissions
            if "secret" in resources:
                role.add_to_policy(iam.PolicyStatement(effect=iam.Effect.ALLOW, actions=secrets_permissions, resources=resources["secret"]))

            # Add Step Functions permissions
            if "states" in resources:
                role.add_to_policy(iam.PolicyStatement(effect=iam.Effect.ALLOW, actions=states_permissions, resources=resources["states"]))

        # Apply tags if provided
        if tags:
            for key, value in tags.items():
                if value:  # Only add non-empty tags
                    Tags.of(role).add(key, value)

        # Apply standard tags
        self.builder.tag_resource(role, formatted_role_name, "AWS IAM Role")

        return role
    

    def _create_legacy_lambda_function(self):
        """Create the lambda function that invokes the domain step function"""
        
        # Create domain lambda function
        lambda_tags = self._create_job_tags('DomainLambda')
        
        lambda_config = LambdaConfig(
            function_name=f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_invoke_legacy_function",
            handler="lambda_function.lambda_handler",
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE_RAW}/invoke_legacy_function",
            timeout=Duration.minutes(15),
            runtime=_lambda.Runtime("python3.13"),
            role=self.role_lambda,
            environment={
                'DATALAKE_AWS_REGION': Stack.of(self).region,
                'DATALAKE_AWS_ACCOUNT_ID': Stack.of(self).account,
                'DATALAKE_ENVIRONMENT': self.PROJECT_CONFIG.environment.value
            }
        )
        
        self.legacy_lambda_function = self.builder.build_lambda_function(lambda_config)
        
        # Add tags to the lambda function
        for key, value in lambda_tags.items():
            Tags.of(self.legacy_lambda_function).add(key, value)
        
        # Add lambda function ARN to outputs for reference by instance stacks
        CfnOutput(
            self, "LegacyLambdaFunctionArn", 
            value=self.legacy_lambda_function.function_arn,
            description="ARN of the lambda function that invokes legacy step function"
        )
        
        CfnOutput(
            self, "LegacyLambdaFunctionName", 
            value=self.legacy_lambda_function.function_name,
            description="Name of the lambda function that invokes legacy step function"
        )

    def _create_domain_lambda_function(self):
        """Create the lambda function that invokes the domain step function"""
        from aws_cdk import aws_lambda as _lambda
        from aje_cdk_libs.models.configs import LambdaConfig
        
        # Create domain lambda function
        lambda_tags = self._create_job_tags('DomainLambda')
        
        lambda_config = LambdaConfig(
            function_name=f"{self.PROJECT_CONFIG.app_config['datasource'].lower()}_invoke_domain_step_function",
            handler="lambda_function.lambda_handler",
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE_RAW}/invoke_domain_step_function",
            timeout=Duration.minutes(15),
            runtime=_lambda.Runtime("python3.13"),
            role=self.role_lambda,
            environment={
                'DATALAKE_AWS_REGION': Stack.of(self).region,
                'DATALAKE_AWS_ACCOUNT_ID': Stack.of(self).account,
                'DATALAKE_ENVIRONMENT': self.PROJECT_CONFIG.environment.value,
                # The domain step function ARN can be set here if known, otherwise it will be constructed dynamically
                # 'DOMAIN_STEP_FUNCTION_ARN': 'arn:aws:states:region:account:stateMachine:domain-step-function-name'
            }
        )
        
        self.domain_lambda_function = self.builder.build_lambda_function(lambda_config)
        
        # Add tags to the lambda function
        for key, value in lambda_tags.items():
            Tags.of(self.domain_lambda_function).add(key, value)
        
        # Add lambda function ARN to outputs for reference by instance stacks
        CfnOutput(
            self, "DomainLambdaFunctionArn", 
            value=self.domain_lambda_function.function_arn,
            description="ARN of the lambda function that invokes domain step function"
        )
        
        CfnOutput(
            self, "DomainLambdaFunctionName", 
            value=self.domain_lambda_function.function_name,
            description="Name of the lambda function that invokes domain step function"
        )

