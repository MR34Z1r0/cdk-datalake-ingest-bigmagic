# main.py - VERSIÓN CORREGIDA

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
import uuid
from datetime import datetime
from typing import Optional

from aje_libs.common.datalake_logger import DataLakeLogger

# Configuración temprana con valores por defecto
DataLakeLogger.configure_global(
    log_level=logging.INFO,
    service_name="extract_data",  # 🔑 Configurar ANTES
    auto_detect_env=True,
    force_local_mode=False
)

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

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Data Lake Extraction Service V2',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:

  # Carga normal (default)
  python main.py --table-name ST_DIM_AFILIACION

  # Primera carga de una tabla (guarda watermark)
  python main.py --table-name ST_DIM_AFILIACION --load-mode initial

  # Reiniciar una tabla corrupta (limpia y recrea watermark)
  python main.py --table-name ST_DIM_AFILIACION --load-mode reset

  # Reprocesar con configuración específica
  python main.py --table-name ST_FACT_RECAUDACION --load-mode reprocess
        """
    )
    
    # Argumentos
    parser.add_argument(
        '--table-name', '-t',
        required=True,
        help='Nombre de la tabla a extraer (ej: ST_DIM_AFILIACION)'
    )
    
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
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Nivel de logging (default: INFO)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validar configuración sin ejecutar extracción'
    )
    
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
        correlation_id=f"{team}-{data_source}-extract_data-{table_name}",
        owner=team
    )

def setup_monitoring(extraction_config: ExtractionConfig, process_guid: str) -> MonitorInterface:
    """Setup monitoring infrastructure"""
    monitor = MonitorFactory.create(
        monitor_type=settings.get('MONITOR_TYPE', 'dynamodb'),
        table_name=extraction_config.dynamo_logs_table,
        project_name=extraction_config.project_name,  # 🔧 AGREGAR esta línea
        sns_topic_arn=extraction_config.topic_arn,
        team=extraction_config.team,
        data_source=extraction_config.data_source,
        endpoint_name=extraction_config.endpoint_name,
        environment=extraction_config.environment,
        process_guid=process_guid
    )
    return monitor

def create_extraction_config(args) -> ExtractionConfig:
    """Create extraction configuration from arguments and environment"""
    # 🔧 NO crear logger aquí todavía
    
    # Cargar configuración base desde .env
    base_config = {
        'PROJECT_NAME': settings.get('PROJECT_NAME'),
        'TEAM': settings.get('TEAM'),
        'DATA_SOURCE': settings.get('DATA_SOURCE'),
        'ENDPOINT_NAME': settings.get('ENDPOINT_NAME'),
        'ENVIRONMENT': settings.get('ENVIRONMENT'),
        'MAX_THREADS': settings.get('MAX_THREADS'),
        'CHUNK_SIZE': settings.get('CHUNK_SIZE'),
        'OUTPUT_FORMAT': settings.get('OUTPUT_FORMAT', 'parquet'),
        'S3_RAW_BUCKET': settings.get('S3_RAW_BUCKET'),
        'DYNAMO_LOGS_TABLE': settings.get('DYNAMO_LOGS_TABLE'),
        'TOPIC_ARN': settings.get('TOPIC_ARN'),
    }
    
    # Determinar load_mode
    load_mode = LoadMode.from_string(args.load_mode)
    
    # Compatibilidad con --force-full-load (deprecated)
    if args.force_full_load:
        # Solo imprimir warning, logger se creará después
        print("⚠️ --force-full-load is DEPRECATED. Use --load-mode reset instead")
        load_mode = LoadMode.RESET
    
    # Imprimir load mode (sin logger todavía)
    print(f"🎯 Load Mode: {load_mode.value.upper()}")
    
    return ExtractionConfig(
        project_name=base_config['PROJECT_NAME'],
        team=base_config['TEAM'],
        data_source=base_config['DATA_SOURCE'],
        endpoint_name=base_config['ENDPOINT_NAME'],
        environment=base_config['ENVIRONMENT'],
        max_threads=base_config['MAX_THREADS'],
        chunk_size=base_config['CHUNK_SIZE'],
        output_format=base_config['OUTPUT_FORMAT'],
        table_name=args.table_name,
        load_mode=load_mode,
        s3_raw_bucket=base_config.get('S3_RAW_BUCKET'),
        dynamo_logs_table=base_config.get('DYNAMO_LOGS_TABLE'),
        topic_arn=base_config.get('TOPIC_ARN'),
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

def print_configuration_summary(extraction_config: ExtractionConfig, process_guid: str):
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
    logger.info(f"🆔 Process GUID: {process_guid}")
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

def print_results_summary(result, logger, process_guid: str):
    """Print extraction results"""
    logger.info("=" * 80)
    logger.info("🎉 EXTRACTION COMPLETED SUCCESSFULLY")
    logger.info(f"🆔 Process GUID: {process_guid}") 
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
    process_guid = str(uuid.uuid4())
    
    try:
        # Setup inicial
        args = parse_arguments()
        extraction_config = create_extraction_config(args)
        
        # ✅ Configurar logging globalmente PRIMERO
        setup_logging(
            args.log_level, 
            extraction_config.table_name, 
            extraction_config.team, 
            extraction_config.data_source
        )
        
        # ✅ Ahora SÍ crear logger y mostrar info
        logger = DataLakeLogger.get_logger(__name__)
        DataLakeLogger.print_environment_info()
        
        logger.info(f"🆔 Process GUID generado: {process_guid}")

        # Setup monitoring
        monitor = setup_monitoring(extraction_config, process_guid)
        
        logger.info("🚀 Starting Data Extraction Process")
        
        validate_environment()
        print_configuration_summary(extraction_config, process_guid)
        
        # Dry run mode
        if args.dry_run:
            logger.info("🧪 DRY RUN MODE - Configuration validation only")
            orchestrator = DataExtractionOrchestrator(extraction_config)
            orchestrator._load_configurations()
            logger.info("✅ Configuration validation successful")
            logger.info(f"🧪 DRY RUN COMPLETED - No data was extracted (process_guid: {process_guid})")
            return 0
         
        # Execute extraction
        orchestrator = DataExtractionOrchestrator(extraction_config, monitor=monitor, process_guid=process_guid)
        result = orchestrator.execute()
        
        # Log success
        print_results_summary(result, logger, process_guid)
        
        return 0
        
    except ConfigurationError as e:
        error_msg = f"Configuration Error: {str(e)}"
        print(f"❌ {error_msg}")
        
        if logger:
            logger.error(f"{error_msg} (process_guid: {process_guid})", exc_info=True)
        
        if monitor:
            monitor.log_error(
                table_name=extraction_config.table_name if extraction_config else "unknown",
                job_name="data_extraction",
                error_message=error_msg,
                metadata={"error_type": "ConfigurationError",
                    "process_guid": process_guid}
            )
        
        return 1
        
    except ConnectionError as e:
        error_msg = f"Connection Error: {str(e)}"
        print(f"❌ {error_msg}")
        
        if logger:
            logger.error(f"{error_msg} (process_guid: {process_guid})", exc_info=True)
        
        if monitor:
            monitor.log_error(
                table_name=extraction_config.table_name if extraction_config else "unknown",
                job_name="data_extraction",
                error_message=error_msg,
                metadata={"error_type": "ConnectionError",
                    "process_guid": process_guid}
            )
        
        return 2
        
    except ExtractionError as e:
        error_msg = f"Extraction Error: {str(e)}"
        print(f"❌ {error_msg}")
        
        if logger:
            logger.error(f"{error_msg} (process_guid: {process_guid})", exc_info=True)
        
        if monitor:
            monitor.log_error(
                table_name=extraction_config.table_name if extraction_config else "unknown",
                job_name="data_extraction",
                error_message=error_msg,
                metadata={"error_type": "ExtractionError",
                    "process_guid": process_guid}
            )
        
        return 3
        
    except LoadError as e:
        error_msg = f"Load Error: {str(e)}"
        print(f"❌ {error_msg}")
        
        if logger:
            logger.error(f"{error_msg} (process_guid: {process_guid})", exc_info=True)
        
        if monitor:
            monitor.log_error(
                table_name=extraction_config.table_name if extraction_config else "unknown",
                job_name="data_extraction",
                error_message=error_msg,
                metadata={"error_type": "LoadError",
                    "process_guid": process_guid}
            )
        
        return 4
        
    except Exception as e:
        error_msg = f"Unexpected Error: {str(e)}"
        print(f"❌ {error_msg}")
        print(f"Traceback: {traceback.format_exc()}")
        
        if logger:
            logger.error(f"{error_msg} (process_guid: {process_guid})", exc_info=True)
        
        if monitor:
            monitor.log_error(
                table_name=extraction_config.table_name if extraction_config else "unknown",
                job_name="data_extraction",
                error_message=error_msg,
                metadata={
                    "error_type": "UnexpectedError",
                    "traceback": traceback.format_exc(),
                    "process_guid": process_guid
                }
            )
        
        return 99


if __name__ == "__main__":
    sys.exit(main())