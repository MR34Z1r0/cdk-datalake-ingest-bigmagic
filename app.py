#!/usr/bin/env python3
import os
import aws_cdk as cdk
from stacks.cdk_datalake_ingest_bigmagic_stack import CdkDatalakeIngestBigmagicStack
from stacks.cdk_datalake_ingest_bigmagic_group_stack import CdkDatalakeIngestBigmagicGroupStack
from stacks.cdk_datalake_ingest_bigmagic_instance_stack import CdkDatalakeIngestBigmagicInstanceStack
from aje_cdk_libs.constants.environments import Environments
from aje_cdk_libs.constants.project_config import ProjectConfig
from constants.paths import Paths
from dotenv import load_dotenv
import csv
import json

load_dotenv() 

# Load environment-specific configuration
environment = os.getenv("ENVIRONMENT", "dev").lower()
env_file = f"{environment}.env"
if os.path.exists(env_file):
    load_dotenv(env_file, override=True)
    print(f"Loaded environment configuration from {env_file}")
else:
    print(f"Warning: Environment file {env_file} not found")

app = cdk.App()

CONFIG = app.node.try_get_context("project_config")
CONFIG["account_id"] = os.getenv("ACCOUNT_ID", None)
CONFIG["region_name"] = os.getenv("REGION_NAME", None)
CONFIG["environment"] = os.getenv("ENVIRONMENT", None) 
CONFIG["separator"] = os.getenv("SEPARATOR", "-") 
project_config = ProjectConfig.from_dict(CONFIG)
project_paths = Paths(project_config.app_config)

# Print some deployment information
print(f"Deploying Datalake Ingest BigMagic to:")
print(f"  Account: {project_config.account_id}")
print(f"  Region: {project_config.region_name}")
print(f"  Environment: {project_config.environment.value}")
      
# Deploy the main BigMagic stack
base_stack = CdkDatalakeIngestBigmagicStack(
    app,
    "CdkDatalakeIngestBigmagicStack",
    project_config,
    env=cdk.Environment(
        account=project_config.account_id,
        region=project_config.region_name
    )
)

# Parse tables.csv to understand process_id distribution and shared tables
tables_data = []
all_process_ids = set()
shared_tables = {}  
shared_job_registry = {}

with open(f'{project_paths.LOCAL_ARTIFACTS_CONFIGURE_CSV}/tables.csv', newline='', encoding='utf-8') as tables_file:
    tables_reader = csv.DictReader(tables_file, delimiter=';')
    for row in tables_reader:
        if row['PROCESS_ID'] and row['SOURCE_SCHEMA'] and row['SOURCE_TABLE']:
            tables_data.append(row)
            # Handle multi-process tables (e.g., "10,20,70")
            if ',' in row['PROCESS_ID']:
                process_ids = [int(pid.strip()) for pid in row['PROCESS_ID'].split(',')]
                shared_tables[row['STAGE_TABLE_NAME']] = process_ids
                all_process_ids.update(process_ids)
            else:
                all_process_ids.add(int(row['PROCESS_ID']))

# Get database names and instances from credentials.csv for current environment
endpoint_names = []
instance_groups = {}  # instance -> list of db_names
current_env = project_config.environment.value.upper()  # Get current environment (DEV/PROD)
#print(f"current_env: {current_env}")
#print(f"shared_tables: {shared_tables}")
with open(f'{project_paths.LOCAL_ARTIFACTS_CONFIGURE_CSV}/credentials.csv', newline='', encoding='utf-8') as creds_file:
    creds_reader = csv.DictReader(creds_file, delimiter=';')
    for row in creds_reader:
        # Only include databases for the current environment
        if row.get('ENV', '').upper() == current_env:
            endpoint_name = row['ENDPOINT_NAME']
            db_name = row['SRC_DB_NAME']
            instance = row['INSTANCE']
            
            endpoint_names.append(endpoint_name)
            
            if instance not in instance_groups:
                instance_groups[instance] = []
            instance_groups[instance].append(endpoint_name)
 
def sanitize_stack_name(process_id, endpoint_name):
    """Sanitize stack name to comply with AWS CloudFormation naming rules"""
    clean_process_id = str(process_id).replace(',', '-').replace('_', '-').replace(' ', '-')
    clean_endpoint_name = str(endpoint_name).replace(',', '-').replace('_', '-').replace(' ', '-')
    stack_name = f"CdkDatalakeIngestGroupStack-{clean_process_id}-{clean_endpoint_name}"
    stack_name = stack_name.replace('--', '-')
    return stack_name

# Deploy individual stacks for each process_id and database combination
deployed_stacks = {}  # Store stack references for dependency management

#print(f"all_process_ids: {all_process_ids}")
# First pass: create all stacks but don't populate registry yet
for process_id in sorted(all_process_ids):  # Sort to ensure consistent order
    for endpoint_name in endpoint_names:
        stack_name = sanitize_stack_name(process_id, endpoint_name)
        
        # Determine if this is the "primary" stack for shared tables
        # (primary = lowest process_id that uses the shared table)
        is_primary_for_shared = {}
        for table_name, table_process_ids in shared_tables.items():
            if process_id in table_process_ids:
                # This is the primary stack if it's the first (lowest) process_id for this table
                is_primary_for_shared[table_name] = (process_id == min(table_process_ids))
        
        # Pre-populate the shared job registry with expected job names
        # This allows secondary stacks to reference jobs that will be created by primary stacks
        for table_name, table_process_ids in shared_tables.items():
            if process_id == min(table_process_ids):  # This is the primary stack
                # Generate expected job names using the naming convention
                datasource = project_config.app_config['datasource'].lower()
                expected_extract_job_name = f"{datasource}_extract_{table_name.lower()}_{endpoint_name.lower()}"
                expected_light_job_name = f"{datasource}_light_transform_{table_name.lower()}_{endpoint_name.lower()}"
                
                registry_key = (table_name, endpoint_name)
                shared_job_registry[registry_key] = {
                    'extract_job_name': expected_extract_job_name,
                    'light_job_name': expected_light_job_name,
                    'created_by_process_id': process_id
                }
        
        # Prepare base stack outputs
        base_stack_outputs = {
            'ArtifactsBucketName': base_stack.s3_artifacts_bucket.bucket_name,
            'RawBucketName': base_stack.s3_raw_bucket.bucket_name,
            'StageBucketName': base_stack.s3_stage_bucket.bucket_name,
            'LandingBucketName': base_stack.s3_landing_bucket.bucket_name,
            'AnalyticsBucketName': base_stack.s3_analytics_bucket.bucket_name,
            'DynamoLogsTableName': base_stack.dynamodb_logs_table.table_name,
            'SnsFailedTopicArn': base_stack.sns_failed_topic.topic_arn,
            'SnsSuccessTopicArn': base_stack.sns_success_topic.topic_arn,
            'RoleExtractArn': base_stack.role_extract.role_arn,
            'RoleLightTransformArn': base_stack.role_light_transform.role_arn,
            'RoleCrawlerArn': base_stack.role_crawler.role_arn,
            'RoleStepFunctionArn': base_stack.role_step_function.role_arn,
            'BaseStepFunctionArn': base_stack.base_step_function.state_machine_arn,
        }
        
        # Add crawler job names for each database
        if hasattr(base_stack, 'crawler_jobs'):
            for db_key, crawler_job in base_stack.crawler_jobs.items():
                base_stack_outputs[f"CrawlerJob{db_key}Name"] = crawler_job.job_name
        
        # Add catalog job names for each database
        if hasattr(base_stack, 'catalog_jobs'):
            for db_key, catalog_job in base_stack.catalog_jobs.items():
                base_stack_outputs[f"CatalogJob{db_key}Name"] = catalog_job.job_name
        
        # Add crawler names for each database
        if hasattr(base_stack, 'crawlers'):
            for db_key, crawler in base_stack.crawlers.items():
                base_stack_outputs[f"Crawler{db_key}Name"] = crawler.name
        
        # Add Glue connection names for cross-stack reference
        if hasattr(base_stack, 'glue_connections'):
            for connection_name, connection in base_stack.glue_connections.items():
                output_name = f"GlueConnection{connection_name.replace('-', '').replace('_', '').title()}Name"
                base_stack_outputs[output_name] = connection.connection_name
        
        group_stack = CdkDatalakeIngestBigmagicGroupStack(
            app,
            stack_name,
            project_config,
            process_id,
            endpoint_name,  # Use db_name instead of src_db_name
            base_stack_outputs=base_stack_outputs,
            shared_table_info=is_primary_for_shared,  # Pass shared table information
            shared_job_registry=shared_job_registry,  # Pass the job registry for cross-stack references
            env=cdk.Environment(
                account=project_config.account_id,
                region=project_config.region_name
            )
        )
        
        # Add explicit dependencies: higher process_id stacks depend on lower process_id stacks
        for dependency_process_id in all_process_ids:
            if dependency_process_id < process_id:
                dependency_stack_name = sanitize_stack_name(dependency_process_id, db_name)
                if dependency_stack_name in deployed_stacks:
                    group_stack.add_dependency(deployed_stacks[dependency_stack_name])
        
        # Store the stack reference
        deployed_stacks[stack_name] = group_stack

#print(f"shared_job_registry: {shared_job_registry}")
#print(f"instance_groups: {instance_groups}")
# Second pass: create instance-level Step Functions for parallel processing
for instance, endpoint_names in instance_groups.items():
    instance_stack_name = f"CdkDatalakeIngestInstanceStack-{instance}"
    
    # Simplified approach: Instance Step Function takes process_id as input
    # No need to collect all group stack references since process_id is dynamic
    
    # Create the instance-level Step Function stack
    instance_stack = CdkDatalakeIngestBigmagicInstanceStack(
        app,
        instance_stack_name,
        project_config,
        instance,
        endpoint_names,
        base_stack_outputs,
        {},  # Empty group stack references since we use dynamic process_id
        env=cdk.Environment(
            account=project_config.account_id,
            region=project_config.region_name
        )
    )
    
    # Add dependencies on all group stacks for this instance
    for endpoint_name in endpoint_names:
        for process_id in sorted(all_process_ids):
            dependency_stack_name = sanitize_stack_name(process_id, endpoint_name)
            if dependency_stack_name in deployed_stacks:
                instance_stack.add_dependency(deployed_stacks[dependency_stack_name])

app.synth()