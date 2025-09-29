#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Extraction Main Entry Point
Flexible data extraction system supporting multiple databases and destinations
"""

import sys
import time
import logging
import traceback
from datetime import datetime
from typing import Optional

from aje_libs.common.datalake_logger import DataLakeLogger
from monitoring.monitor_factory import MonitorFactory
from interfaces.monitor_interface import MonitorInterface
from core.orchestrator import DataExtractionOrchestrator
from models.extraction_config import ExtractionConfig
from exceptions.custom_exceptions import (
    ConfigurationError, ConnectionError, 
    ExtractionError, LoadError
)
from config.settings import settings
import argparse

def parse_arguments():
    """Parse command line arguments with .env fallback"""
    parser = argparse.ArgumentParser(description='Data Extraction Tool')
    
    # Cargar valores del .env como defaults
    from config.settings import settings
    base_config = settings.get_all()
    
    # Hacer los argumentos opcionales y usar valores del .env como default
    parser.add_argument('--table-name', 
                       default=base_config.get('TABLE_NAME'), 
                       help='Table name to extract')
    parser.add_argument('--team', 
                       default=base_config.get('TEAM'), 
                       help='Team name')
    parser.add_argument('--data-source', 
                       default=base_config.get('DATA_SOURCE'), 
                       help='Data source name')
    parser.add_argument('--environment', 
                       default=base_config.get('ENVIRONMENT', 'DEV'), 
                       help='Environment (DEV, PROD)')
    parser.add_argument('--force-full-load', action='store_true', help='Force full load')
    parser.add_argument('--max-threads', type=int, help='Maximum threads')
    parser.add_argument('--output-format', 
                       choices=['parquet', 'csv', 'json'], 
                       default='parquet')
    parser.add_argument('--chunk-size', type=int, help='Chunk size')
    parser.add_argument('--log-level', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default='INFO')
    parser.add_argument('--dry-run', action='store_true', help='Validate only')
    
    return parser.parse_args()

def setup_logging(log_level: str, table_name: str, team: str, data_source: str):
    """Setup DataLakeLogger configuration"""
    log_level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR
    }
    
    DataLakeLogger.configure_global(
        log_level=log_level_map.get(log_level, logging.INFO),
        service_name="extract_data",
        correlation_id=f"extract-{table_name}-{int(time.time())}",
        owner=team,
        auto_detect_env=True,
        force_local_mode=False
    )

def setup_monitoring(extraction_config: ExtractionConfig) -> MonitorInterface:
    """
    Setup monitoring system - ÚNICO PUNTO DE ENTRADA para logs de DynamoDB
    """
    return MonitorFactory.create(
        'dynamodb',
        table_name=extraction_config.dynamo_logs_table,
        project_name=extraction_config.data_source,
        sns_topic_arn=extraction_config.topic_arn,
        team=extraction_config.team,
        data_source=extraction_config.data_source,
        environment=extraction_config.environment
    )

def create_extraction_config(args) -> ExtractionConfig:
    """Create extraction configuration"""
    base_config = settings.get_all()
    
    return ExtractionConfig(
        table_name=args.table_name,
        team=args.team,
        data_source=args.data_source,
        environment=args.environment,
        force_full_load=args.force_full_load or base_config.get('force_full_load', False),
        max_threads=args.max_threads or base_config.get('max_threads', 4),
        output_format=args.output_format,
        chunk_size=args.chunk_size or base_config.get('chunk_size', 10000),
        # ... resto de configuración
        dynamo_logs_table=base_config.get('dynamo_logs_table'),
        topic_arn=base_config.get('sns_topic_arn'),
    )

def validate_environment():
    """Validate required environment variables"""
    logger = DataLakeLogger.get_logger(__name__)
    required_vars = ['S3_RAW_BUCKET', 'DYNAMO_LOGS_TABLE']
    
    missing = [var for var in required_vars if not settings.get(var.lower())]
    
    if missing:
        raise ConfigurationError(f"Missing required variables: {', '.join(missing)}")
    
    logger.info("✅ Environment validation passed")

def print_configuration_summary(extraction_config: ExtractionConfig):
    """Print configuration summary"""
    logger = DataLakeLogger.get_logger(__name__)
    
    logger.info("=" * 80)
    logger.info("📋 DATA EXTRACTION CONFIGURATION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"📊 Table: {extraction_config.table_name}")
    logger.info(f"👥 Team: {extraction_config.team}")
    logger.info(f"📡 Source: {extraction_config.data_source}")
    logger.info(f"🌍 Environment: {extraction_config.environment}")
    logger.info(f"📁 Format: {extraction_config.output_format}")
    logger.info(f"🧵 Threads: {extraction_config.max_threads}")
    logger.info(f"📦 Chunk Size: {extraction_config.chunk_size:,}")
    logger.info(f"🔄 Full Load: {extraction_config.force_full_load}")
    logger.info("=" * 80)

def print_results_summary(result, logger):
    """Print extraction results"""
    logger.info("=" * 80)
    logger.info("🎉 EXTRACTION COMPLETED SUCCESSFULLY")
    logger.info(f"📊 Records: {result.records_extracted:,}")
    logger.info(f"📁 Files: {len(result.files_created)}")
    logger.info(f"⏱️ Time: {result.execution_time_seconds:.2f}s")
    logger.info(f"🎯 Strategy: {result.strategy_used}")
    logger.info("=" * 80)

def main():
    """Main entry point with integrated logging"""
    logger = None
    monitor: Optional[MonitorInterface] = None
    extraction_config = None
    process_id = None
    
    try:
        # Setup inicial
        args = parse_arguments()
        extraction_config = create_extraction_config(args)
        setup_logging(args.log_level, extraction_config.table_name, 
                     extraction_config.team, extraction_config.data_source)
        
        logger = DataLakeLogger.get_logger(__name__)
        DataLakeLogger.print_environment_info()
        
        # ÚNICO monitor centralizado
        monitor = setup_monitoring(extraction_config)
        
        logger.info("🚀 Starting Data Extraction Process", {
            "table": extraction_config.table_name,
            "team": extraction_config.team,
            "data_source": extraction_config.data_source
        })
        
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
        
        # Log start SOLO a través del monitor
        process_id = monitor.log_start(
            table_name=extraction_config.table_name,
            strategy="auto",  # Se determinará después
            metadata={
                "start_time": datetime.now().isoformat(),
                "force_full_load": extraction_config.force_full_load,
                "output_format": extraction_config.output_format,
                "chunk_size": extraction_config.chunk_size,
                "max_threads": extraction_config.max_threads
            }
        )
        
        logger.info(f"⚡ Starting data extraction...", {"process_id": process_id})
        
        # Execute extraction con el monitor integrado
        orchestrator = DataExtractionOrchestrator(extraction_config, monitor=monitor)
        result = orchestrator.execute()
        
        # Log success SOLO a través del monitor
        print_results_summary(result, logger)
        
        # NO llamar a dynamo_logger.log_success, ya se hace en orchestrator
        # El orchestrator internamente llama a monitor.log_success
        
        return 0
        
    except KeyboardInterrupt:
        error_msg = "Process interrupted by user"
        if logger:
            logger.warning(f"⚠️ {error_msg}")
        if monitor and extraction_config:
            monitor.log_warning(extraction_config.table_name, error_msg, 
                              {"interrupted_at": datetime.now().isoformat()})
        return 130
        
    except (ConfigurationError, ConnectionError, ExtractionError, LoadError) as e:
        error_type = type(e).__name__
        error_msg = f"{error_type}: {str(e)}"
        
        if logger: 
            logger.error(f"❌ {error_msg}")
        else: 
            print(f"ERROR: {error_msg}")
        
        # Log error SOLO a través del monitor
        if monitor and extraction_config:
            monitor.log_error(
                table_name=extraction_config.table_name,
                error_message=error_msg,
                metadata={
                    "error_type": error_type,
                    "failed_at": datetime.now().isoformat(),
                    "process_id": process_id
                }
            )
        
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
        
        # Log error SOLO a través del monitor
        if monitor and extraction_config:
            monitor.log_error(
                table_name=extraction_config.table_name,
                error_message=error_msg,
                metadata={
                    "error_type": "UnexpectedError",
                    "failed_at": datetime.now().isoformat(),
                    "traceback": traceback.format_exc()[:1000],  # Limitado
                    "process_id": process_id
                }
            )
            
        return 99

if __name__ == '__main__':
    sys.exit(main())