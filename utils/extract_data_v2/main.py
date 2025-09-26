#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Extraction Main Entry Point
Flexible data extraction system supporting multiple databases and destinations
"""

import sys
import argparse
import traceback
from pathlib import Path

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from models.extraction_config import ExtractionConfig
from core.orchestrator import DataExtractionOrchestrator
from exceptions.custom_exceptions import *
from config.settings import settings
from aje_libs.common.logger import custom_logger, set_logger_config

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
    parser.add_argument(
        '-t', '--table-name',
        required=True,
        help='Target table name to extract'
    )
    
    # Optional arguments (mainly for local development)
    parser.add_argument(
        '--project-name',
        help='Project name (overrides environment/config)'
    )
    
    parser.add_argument(
        '--team',
        help='Team name (overrides environment/config)'
    )
    
    parser.add_argument(
        '--data-source',
        help='Data source name (overrides environment/config)'
    )
    
    parser.add_argument(
        '--endpoint-name',
        help='Database endpoint name (overrides environment/config)'
    )
    
    parser.add_argument(
        '--environment',
        help='Environment (DEV, PROD, etc.) (overrides environment/config)'
    )
    
    parser.add_argument(
        '--force-full-load',
        action='store_true',
        help='Force full load even for incremental tables'
    )
    
    parser.add_argument(
        '--max-threads',
        type=int,
        default=6,
        help='Maximum number of threads for parallel processing (default: 6)'
    )
    
    parser.add_argument(
        '--output-format',
        choices=['parquet', 'csv', 'json'],
        default='parquet',
        help='Output file format (default: parquet)'
    )
    
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=500000,
        help='Chunk size for large table processing (default: 500000)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate configuration without executing extraction'
    )
    
    return parser.parse_args()

def setup_logging(log_level: str):
    """Setup logging configuration"""
    import logging
    
    log_level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR
    }
    
    set_logger_config(
        log_level=log_level_map.get(log_level, logging.INFO),
        service='extract_data'
    )

def create_extraction_config(args) -> ExtractionConfig:
    """Create extraction configuration from arguments and settings"""
    
    # Get base configuration from settings
    base_config = settings.get_all()
    
    # Override with command line arguments if provided
    overrides = {}
    
    if args.project_name:
        overrides['PROJECT_NAME'] = args.project_name
    if args.team:
        overrides['TEAM'] = args.team
    if args.data_source:
        overrides['DATA_SOURCE'] = args.data_source
    if args.endpoint_name:
        overrides['ENDPOINT_NAME'] = args.endpoint_name
    if args.environment:
        overrides['ENVIRONMENT'] = args.environment
    if args.force_full_load:
        overrides['FORCE_FULL_LOAD'] = True
    if args.max_threads:
        overrides['MAX_THREADS'] = args.max_threads
    if args.output_format:
        overrides['OUTPUT_FORMAT'] = args.output_format
    if args.chunk_size:
        overrides['CHUNK_SIZE'] = args.chunk_size
    
    # Update settings with overrides
    if overrides:
        settings.update(overrides)
        base_config = settings.get_all()
    
    # Set table name
    base_config['TABLE_NAME'] = args.table_name
    
    # Create extraction config
    extraction_config = ExtractionConfig(
        project_name=base_config['PROJECT_NAME'],
        team=base_config['TEAM'],
        data_source=base_config['DATA_SOURCE'],
        endpoint_name=base_config['ENDPOINT_NAME'],
        environment=base_config['ENVIRONMENT'],
        table_name=base_config['TABLE_NAME'],
        s3_raw_bucket=base_config.get('S3_RAW_BUCKET'),
        dynamo_logs_table=base_config.get('DYNAMO_LOGS_TABLE'),
        topic_arn=base_config.get('TOPIC_ARN'),
        max_threads=base_config.get('MAX_THREADS', 6),
        chunk_size=base_config.get('CHUNK_SIZE', 500000),
        force_full_load=base_config.get('FORCE_FULL_LOAD', False),
        output_format=base_config.get('OUTPUT_FORMAT', 'parquet')
    )
    
    return extraction_config

def validate_environment():
    """Validate that the environment is properly configured"""
    logger = custom_logger(__name__)
    
    # Check required settings
    required_settings = [
        'PROJECT_NAME', 'TEAM', 'DATA_SOURCE', 
        'ENDPOINT_NAME', 'ENVIRONMENT'
    ]
    
    missing_settings = []
    for setting in required_settings:
        if not settings.get(setting):
            missing_settings.append(setting)
    
    if missing_settings:
        raise ConfigurationError(
            f"Missing required configuration: {', '.join(missing_settings)}"
        )
    
    logger.info("Environment validation passed")

def print_configuration_summary(extraction_config: ExtractionConfig):
    """Print configuration summary"""
    logger = custom_logger(__name__)
    
    logger.info("=" * 80)
    logger.info("DATA EXTRACTION CONFIGURATION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Table Name: {extraction_config.table_name}")
    logger.info(f"Project: {extraction_config.project_name}")
    logger.info(f"Team: {extraction_config.team}")
    logger.info(f"Data Source: {extraction_config.data_source}")
    logger.info(f"Endpoint: {extraction_config.endpoint_name}")
    logger.info(f"Environment: {extraction_config.environment}")
    logger.info(f"Output Format: {extraction_config.output_format}")
    logger.info(f"Max Threads: {extraction_config.max_threads}")
    logger.info(f"Chunk Size: {extraction_config.chunk_size:,}")
    logger.info(f"Force Full Load: {extraction_config.force_full_load}")
    logger.info(f"Running in AWS: {settings.is_aws_glue}")
    logger.info("=" * 80)

def main():
    """Main entry point"""
    logger = None
    
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Setup logging
        setup_logging(args.log_level)
        logger = custom_logger(__name__)
        
        logger.info("Starting Data Extraction Process")
        
        # Validate environment
        validate_environment()
        
        # Create extraction configuration
        extraction_config = create_extraction_config(args)
        
        # Print configuration summary
        print_configuration_summary(extraction_config)
        
        # Dry run mode - just validate configuration
        if args.dry_run:
            logger.info("DRY RUN MODE - Configuration validation only")
            
            # Create orchestrator to validate configuration
            orchestrator = DataExtractionOrchestrator(extraction_config)
            orchestrator._load_configurations()  # This will validate configs
            
            logger.info("✅ Configuration validation successful")
            logger.info("DRY RUN COMPLETED - No data was extracted")
            return 0
        
        # Execute extraction
        logger.info("Initializing Data Extraction Orchestrator")
        orchestrator = DataExtractionOrchestrator(extraction_config)
        
        logger.info("Starting data extraction...")
        result = orchestrator.execute()
        
        # Print results
        logger.info("=" * 80)
        logger.info("EXTRACTION COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Records Extracted: {result.records_extracted:,}")
        logger.info(f"Files Created: {len(result.files_created)}")
        logger.info(f"Execution Time: {result.execution_time_seconds:.2f} seconds")
        logger.info(f"Strategy Used: {result.strategy_used}")
        
        if result.files_created:
            logger.info("Files created:")
            for file_path in result.files_created[:10]:  # Show first 10 files
                logger.info(f"  - {file_path}")
            if len(result.files_created) > 10:
                logger.info(f"  ... and {len(result.files_created) - 10} more files")
        
        logger.info("=" * 80)
        
        return 0
        
    except KeyboardInterrupt:
        if logger:
            logger.warning("Process interrupted by user")
        else:
            print("Process interrupted by user")
        return 130
        
    except ConfigurationError as e:
        error_msg = f"Configuration Error: {e}"
        if logger:
            logger.error(error_msg)
        else:
            print(f"ERROR: {error_msg}")
        return 1
        
    except ConnectionError as e:
        error_msg = f"Connection Error: {e}"
        if logger:
            logger.error(error_msg)
        else:
            print(f"ERROR: {error_msg}")
        return 2
        
    except ExtractionError as e:
        error_msg = f"Extraction Error: {e}"
        if logger:
            logger.error(error_msg)
        else:
            print(f"ERROR: {error_msg}")
        return 3
        
    except LoadError as e:
        error_msg = f"Load Error: {e}"
        if logger:
            logger.error(error_msg)
        else:
            print(f"ERROR: {error_msg}")
        return 4
        
    except Exception as e:
        error_msg = f"Unexpected Error: {e}"
        if logger:
            logger.error(error_msg)
            logger.error(traceback.format_exc())
        else:
            print(f"ERROR: {error_msg}")
            traceback.print_exc()
        return 99

if __name__ == '__main__':
    sys.exit(main())