#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Extraction Main Entry Point
Flexible data extraction system supporting multiple databases and destinations
"""

import sys
import argparse
import traceback
import time
import logging
from pathlib import Path
from datetime import datetime

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from models.extraction_config import ExtractionConfig
from core.orchestrator import DataExtractionOrchestrator
from exceptions.custom_exceptions import *
from config.settings import settings
from aje_libs.common.datalake_logger import DataLakeLogger
from aje_libs.common.dynamodb_logger import DynamoDBLogger

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Extract data from source database and load to destination',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract specific table
  python main.py -t STG_USERS

  # Extract with custom parameters (for local testing)
  python main.py -t STG_USERS --project-name myproject --team myteam
  
  # Force full load
  python main.py -t STG_USERS --force-full-load
        """
    )
    
    # Required arguments
    parser.add_argument('-t', '--table-name', required=True, help='Target table name to extract')
    
    # Optional arguments (mainly for local development)
    parser.add_argument('--project-name', help='Project name (overrides environment/config)')
    parser.add_argument('--team', help='Team name (overrides environment/config)')
    parser.add_argument('--data-source', help='Data source name (overrides environment/config)')
    parser.add_argument('--endpoint-name', help='Database endpoint name (overrides environment/config)')
    parser.add_argument('--environment', help='Environment (DEV, PROD, etc.) (overrides environment/config)')
    parser.add_argument('--force-full-load', action='store_true', help='Force full load even for incremental tables')
    parser.add_argument('--max-threads', type=int, help='Maximum number of threads for parallel processing (overrides .env)')
    parser.add_argument('--output-format', choices=['parquet', 'csv', 'json'], default='parquet', help='Output file format (default: parquet)')
    parser.add_argument('--chunk-size', type=int, help='Chunk size for large table processing (overrides .env)')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO', help='Logging level (default: INFO)')
    parser.add_argument('--dry-run', action='store_true', help='Validate configuration without executing extraction')
    
    return parser.parse_args()

def setup_logging(log_level: str, table_name: str, team: str, data_source: str):
    """Setup logging configuration using DataLakeLogger"""
    log_level_map = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING, 'ERROR': logging.ERROR}
    
    DataLakeLogger.configure_global(
        log_level=log_level_map.get(log_level, logging.INFO),
        service_name="extract_data",
        correlation_id=f"extract-{table_name}-{int(time.time())}",
        owner=team,
        auto_detect_env=True,
        force_local_mode=False
    )

def setup_dynamodb_logger(args, extraction_config) -> DynamoDBLogger:
    """Setup DynamoDB Logger"""
    return DynamoDBLogger(
        table_name=extraction_config.dynamo_logs_table,
        sns_topic_arn=extraction_config.topic_arn,
        team=extraction_config.team,
        data_source=extraction_config.data_source,
        flow_name="extract_data",
        environment=extraction_config.environment,
        logger_name=f"{extraction_config.team}-{extraction_config.data_source}-extract"
    )

def create_extraction_config(args) -> ExtractionConfig:
    """Create extraction configuration from arguments and settings"""
    base_config = settings.get_all()
    
    # Override with command line arguments if provided
    overrides = {}
    if args.project_name: overrides['PROJECT_NAME'] = args.project_name
    if args.team: overrides['TEAM'] = args.team
    if args.data_source: overrides['DATA_SOURCE'] = args.data_source
    if args.endpoint_name: overrides['ENDPOINT_NAME'] = args.endpoint_name
    if args.environment: overrides['ENVIRONMENT'] = args.environment
    if args.force_full_load: overrides['FORCE_FULL_LOAD'] = True
    if args.max_threads: overrides['MAX_THREADS'] = args.max_threads
    if args.output_format: overrides['OUTPUT_FORMAT'] = args.output_format
    if args.chunk_size: overrides['CHUNK_SIZE'] = args.chunk_size
    
    if overrides:
        settings.update(overrides)
        base_config = settings.get_all()
    
    base_config['TABLE_NAME'] = args.table_name
    
    return ExtractionConfig(
        project_name=base_config['PROJECT_NAME'],
        team=base_config['TEAM'],
        data_source=base_config['DATA_SOURCE'],
        endpoint_name=base_config['ENDPOINT_NAME'],
        environment=base_config['ENVIRONMENT'],
        table_name=base_config['TABLE_NAME'],
        s3_raw_bucket=base_config.get('S3_RAW_BUCKET'),
        dynamo_logs_table=base_config.get('DYNAMO_LOGS_TABLE'),
        topic_arn=base_config.get('TOPIC_ARN'),
        max_threads=base_config['MAX_THREADS'],
        chunk_size=base_config['CHUNK_SIZE'],
        force_full_load=base_config.get('FORCE_FULL_LOAD', False),
        output_format=base_config.get('OUTPUT_FORMAT', 'parquet')
    )

def validate_environment():
    """Validate that the environment is properly configured"""
    logger = DataLakeLogger.get_logger(__name__)
    required_settings = ['PROJECT_NAME', 'TEAM', 'DATA_SOURCE', 'ENDPOINT_NAME', 'ENVIRONMENT']
    
    missing_settings = [setting for setting in required_settings if not settings.get(setting)]
    
    if missing_settings:
        raise ConfigurationError(f"Missing required configuration: {', '.join(missing_settings)}")
    
    logger.info("✅ Environment validation passed")

def print_configuration_summary(extraction_config: ExtractionConfig):
    """Print configuration summary"""
    logger = DataLakeLogger.get_logger(__name__)
    
    logger.info("=" * 80)
    logger.info("📋 DATA EXTRACTION CONFIGURATION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"📊 Table: {extraction_config.table_name} | 👥 Team: {extraction_config.team}")
    logger.info(f"🏗️ Project: {extraction_config.project_name} | 📡 Source: {extraction_config.data_source}")
    logger.info(f"🔗 Endpoint: {extraction_config.endpoint_name} | 🌍 Env: {extraction_config.environment}")
    logger.info(f"📁 Format: {extraction_config.output_format} | 🧵 Threads: {extraction_config.max_threads} | 📦 Chunk: {extraction_config.chunk_size:,}")
    logger.info(f"🔄 Full Load: {extraction_config.force_full_load} | ☁️ AWS: {settings.is_aws_glue}")
    logger.info("=" * 80)

def print_results_summary(result, logger):
    """Print extraction results summary"""
    logger.info("=" * 80)
    logger.info("🎉 EXTRACTION COMPLETED SUCCESSFULLY")
    logger.info(f"📊 Records: {result.records_extracted:,} | 📁 Files: {len(result.files_created)} | ⏱️ Time: {result.execution_time_seconds:.2f}s | 🎯 Strategy: {result.strategy_used}")
    if result.files_created:
        for file_path in result.files_created[:5]:  # Show first 5 files
            logger.info(f"  📎 {file_path}")
        if len(result.files_created) > 5:
            logger.info(f"  📎 ... and {len(result.files_created) - 5} more files")
    logger.info("=" * 80)

def log_error_to_dynamo(dynamo_logger, extraction_config, error_type: str, error_msg: str):
    """Helper function to log errors to DynamoDB"""
    if dynamo_logger and extraction_config:
        dynamo_logger.log_failure(
            table_name=extraction_config.table_name,
            error_message=error_msg,
            job_name="extract_data_main",
            context={
                "error_type": error_type,
                "failed_at": datetime.now().isoformat()
            }
        )

def main():
    """Main entry point with integrated logging"""
    logger = None
    dynamo_logger = None
    extraction_config = None
    
    try:
        # Setup
        args = parse_arguments()
        extraction_config = create_extraction_config(args)
        setup_logging(args.log_level, extraction_config.table_name, extraction_config.team, extraction_config.data_source)
        logger = DataLakeLogger.get_logger(__name__)
        DataLakeLogger.print_environment_info()
        
        logger.info("🚀 Starting Data Extraction Process", {
            "table": extraction_config.table_name,
            "team": extraction_config.team,
            "data_source": extraction_config.data_source
        })
        
        dynamo_logger = setup_dynamodb_logger(args, extraction_config)
        validate_environment()
        print_configuration_summary(extraction_config)
        
        # Dry run mode
        if args.dry_run:
            logger.info("🧪 DRY RUN MODE - Configuration validation only")
            orchestrator = DataExtractionOrchestrator(extraction_config)
            orchestrator._load_configurations()
            logger.info("✅ Configuration validation successful")
            logger.info("🧪 DRY RUN COMPLETED - No data was extracted")
            return 0
        
        # Register start in DynamoDB
        process_id = dynamo_logger.log_start(
            table_name=extraction_config.table_name,
            job_name="extract_data_main",
            context={
                "start_time": datetime.now().isoformat(),
                "force_full_load": extraction_config.force_full_load,
                "output_format": extraction_config.output_format,
                "chunk_size": extraction_config.chunk_size,
                "max_threads": extraction_config.max_threads
            }
        )
        
        # Execute extraction
        logger.info("🔧 Initializing Data Extraction Orchestrator")
        orchestrator = DataExtractionOrchestrator(extraction_config)
        
        logger.info("⚡ Starting data extraction...", {
            "table": extraction_config.table_name,
            "process_id": process_id
        })
        
        result = orchestrator.execute()
        
        # Success
        print_results_summary(result, logger)
        dynamo_logger.log_success(
            table_name=extraction_config.table_name,
            job_name="extract_data_main",
            context={
                "end_time": datetime.now().isoformat(),
                "records_extracted": result.records_extracted,
                "files_created_count": len(result.files_created),
                "execution_time_seconds": result.execution_time_seconds,
                "strategy_used": result.strategy_used
            }
        )
        
        return 0
        
    except KeyboardInterrupt:
        error_msg = "Process interrupted by user"
        if logger: logger.warning(f"⚠️ {error_msg}")
        if dynamo_logger and extraction_config:
            dynamo_logger.log_warning(extraction_config.table_name, error_msg, "extract_data_main")
        return 130
        
    except (ConfigurationError, ConnectionError, ExtractionError, LoadError) as e:
        error_type = type(e).__name__
        error_msg = f"{error_type}: {str(e)}"
        
        if logger: logger.error(f"❌ {error_msg}")
        else: print(f"ERROR: {error_msg}")
        
        log_error_to_dynamo(dynamo_logger, extraction_config, error_type, error_msg)
        
        # Return different codes for different error types
        error_codes = {
            'ConfigurationError': 1,
            'ConnectionError': 2, 
            'ExtractionError': 3,
            'LoadError': 4
        }
        return error_codes.get(error_type, 99)
        
    except Exception as e:
        error_msg = f"Unexpected Error: {str(e)}"
        
        if logger:
            logger.error(f"💥 {error_msg}")
            logger.error(traceback.format_exc())
        else:
            print(f"ERROR: {error_msg}")
            traceback.print_exc()
        
        log_error_to_dynamo(dynamo_logger, extraction_config, "UnexpectedError", error_msg)
        return 99

if __name__ == '__main__':
    sys.exit(main())