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
from models.load_mode import LoadMode
import argparse

logger = DataLakeLogger.get_logger(__name__)
DataLakeLogger.print_environment_info()

def parse_arguments():
    """Parse command line arguments - only table name required"""
    parser = argparse.ArgumentParser(description='Data Extraction Tool')
    
    # ÚNICO argumento requerido
    parser.add_argument('--table-name', '-t',
                       required=True,
                       help='Table name to extract')
    
    parser.add_argument(
        '--load-mode', '-m',
        type=str,
        choices=['initial', 'normal', 'reset', 'reprocess'],
        default='normal',
        help="""Modo de carga:
        - initial: Primera carga (full + guarda watermark)
        - normal: Carga regular (incremental desde watermark o full sin watermark) [DEFAULT]
        - reset: Reiniciar (limpia watermark + full + guarda nuevo watermark)
        - reprocess: Reprocesar con configuración específica
        """
    )
    
    # Argumentos opcionales para debugging/override (raramente usados)
    parser.add_argument('--log-level', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default='INFO',
                       help='Log level (default: INFO)')
    parser.add_argument('--dry-run', 
                       action='store_true', 
                       help='Validate configuration only, do not extract')

    # ⚠️ DEPRECATED: Mantener por compatibilidad
    parser.add_argument(
        '--force-full-load',
        action='store_true',
        help='[DEPRECATED] Usar --load-mode reset en su lugar'
    )
    
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
        correlation_id=f"{team}-{data_source}-extract_data-{table_name}",
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
        endpoint_name=extraction_config.endpoint_name,
        environment=extraction_config.environment
    )

def create_extraction_config(args) -> ExtractionConfig:
    """
    Create extraction configuration from .env
    Only table_name comes from CLI args, everything else from .env
    """
    base_config = settings.get_all()
    
    # Validar que existan las variables críticas del .env
    required_env_vars = ['PROJECT_NAME', 'TEAM', 'DATA_SOURCE', 'ENDPOINT_NAME', 
                         'ENVIRONMENT', 'MAX_THREADS', 'CHUNK_SIZE']
    missing_vars = [var for var in required_env_vars if not base_config.get(var)]
    
    if missing_vars:
        raise ConfigurationError(
            f"Missing required environment variables in .env: {', '.join(missing_vars)}"
        )
    
    load_mode = LoadMode.from_string(args.load_mode)
    
    if args.force_full_load:
        logger.warning("⚠️ --force-full-load is DEPRECATED. Use --load-mode reset instead")
        load_mode = LoadMode.RESET
    
    logger.info(f"🎯 Load Mode: {load_mode.value.upper()}")

    return ExtractionConfig(
        # Desde .env
        project_name=base_config['PROJECT_NAME'],
        team=base_config['TEAM'],
        data_source=base_config['DATA_SOURCE'],
        endpoint_name=base_config['ENDPOINT_NAME'],
        environment=base_config['ENVIRONMENT'],
        max_threads=base_config['MAX_THREADS'],
        chunk_size=base_config['CHUNK_SIZE'],
        output_format=base_config['OUTPUT_FORMAT'],
        
        # Desde argumentos
        table_name=args.table_name,
        load_mode=load_mode,
        
        # Opcionales
        s3_raw_bucket=base_config.get('S3_RAW_BUCKET'),
        dynamo_logs_table=base_config.get('DYNAMO_LOGS_TABLE'),
        topic_arn=base_config.get('TOPIC_ARN'),
        
        # Deprecated pero mantener por compatibilidad
        force_full_load=args.force_full_load
    )

def validate_environment():
    """Validate required environment variables"""
    logger = DataLakeLogger.get_logger(__name__)
    required_vars = ['S3_RAW_BUCKET', 'DYNAMO_LOGS_TABLE']
    
    missing = [var for var in required_vars if not settings.get(var)]
    
    if missing:
        raise ConfigurationError(f"Missing required variables: {', '.join(missing)}")
    
    logger.info("✅ Environment validation passed")

def print_configuration_summary(extraction_config: ExtractionConfig):
    """Print configuration summary"""
    logger = DataLakeLogger.get_logger(__name__)
    
    # Emoji según load mode
    mode_emoji = {
        LoadMode.INITIAL: "🆕",
        LoadMode.NORMAL: "🔄",
        LoadMode.RESET: "♻️",
        LoadMode.REPROCESS: "🔁"
    }
    
    emoji = mode_emoji.get(extraction_config.load_mode, "❓")
    
    logger.info("=" * 80)
    logger.info("📋 DATA EXTRACTION CONFIGURATION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"📊 Table: {extraction_config.table_name}")
    logger.info(f"👥 Team: {extraction_config.team}")
    logger.info(f"📡 Source: {extraction_config.data_source}")
    logger.info(f"🔌 Endpoint: {extraction_config.endpoint_name}")
    logger.info(f"🌍 Environment: {extraction_config.environment}")
    logger.info(f"{emoji} Load Mode: {extraction_config.load_mode.value.upper()}")
    logger.info(f"📁 Format: {extraction_config.output_format}")
    logger.info(f"🧵 Threads: {extraction_config.max_threads}")
    logger.info(f"📦 Chunk Size: {extraction_config.chunk_size:,}")
    logger.info("=" * 80)
    
    # Explicar el load mode
    mode_descriptions = {
        LoadMode.INITIAL: "Primera carga - Extrae TODO y guarda watermark",
        LoadMode.NORMAL: "Carga regular - Incremental desde watermark o full sin watermark",
        LoadMode.RESET: "Reinicio - Limpia watermark, extrae TODO y guarda nuevo watermark",
        LoadMode.REPROCESS: "Reprocesamiento - Usa configuración específica (ej: rango de fechas)"
    }
    
    logger.info(f"ℹ️  {mode_descriptions.get(extraction_config.load_mode, 'Modo desconocido')}")
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
        
        # Configurar logging globalmente
        setup_logging(
            args.log_level, 
            extraction_config.table_name, 
            extraction_config.team, 
            extraction_config.data_source
        )
        
        # 🔧 CORRECCIÓN: Obtener logger DESPUÉS de configurar
        logger = DataLakeLogger.get_logger(__name__)
        
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
         
        # Execute extraction con el monitor integrado
        orchestrator = DataExtractionOrchestrator(extraction_config, monitor=monitor)
        result = orchestrator.execute()
        
        # Log success SOLO a través del monitor
        print_results_summary(result, logger)
        
        return 0
        
    except ConfigurationError as e:
        error_msg = f"Configuration Error: {str(e)}"
        print(f"❌ {error_msg}")
        
        if logger:
            logger.error(error_msg, exc_info=True)
        
        if monitor:
            monitor.log_error(
                table_name=extraction_config.table_name if extraction_config else "unknown",
                job_name="data_extraction",
                error_message=error_msg,
                context={"error_type": "ConfigurationError"}
            )
        
        return 1
        
    except ConnectionError as e:
        error_msg = f"Connection Error: {str(e)}"
        print(f"❌ {error_msg}")
        
        if logger:
            logger.error(error_msg, exc_info=True)
        
        if monitor:
            monitor.log_error(
                table_name=extraction_config.table_name if extraction_config else "unknown",
                job_name="data_extraction",
                error_message=error_msg,
                context={"error_type": "ConnectionError"}
            )
        
        return 2
        
    except ExtractionError as e:
        error_msg = f"Extraction Error: {str(e)}"
        print(f"❌ {error_msg}")
        
        if logger:
            logger.error(error_msg, exc_info=True)
        
        if monitor:
            monitor.log_error(
                table_name=extraction_config.table_name if extraction_config else "unknown",
                job_name="data_extraction",
                error_message=error_msg,
                context={"error_type": "ExtractionError"}
            )
        
        return 3
        
    except LoadError as e:
        error_msg = f"Load Error: {str(e)}"
        print(f"❌ {error_msg}")
        
        if logger:
            logger.error(error_msg, exc_info=True)
        
        if monitor:
            monitor.log_error(
                table_name=extraction_config.table_name if extraction_config else "unknown",
                job_name="data_extraction",
                error_message=error_msg,
                context={"error_type": "LoadError"}
            )
        
        return 4
        
    except Exception as e:
        error_msg = f"Unexpected Error: {str(e)}"
        print(f"❌ {error_msg}")
        print(f"Traceback: {traceback.format_exc()}")
        
        if logger:
            logger.error(error_msg, exc_info=True)
        
        if monitor:
            monitor.log_error(
                table_name=extraction_config.table_name if extraction_config else "unknown",
                job_name="data_extraction",
                error_message=error_msg,
                context={
                    "error_type": "UnexpectedError",
                    "traceback": traceback.format_exc()
                }
            )
        
        return 99
    
if __name__ == '__main__':
    sys.exit(main())