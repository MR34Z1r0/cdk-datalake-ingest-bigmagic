# utils/extract_data_v2/aje_libs/common/datalake_logger.py

import os
import logging
import datetime as dt
from typing import Optional, Union, Dict, Any
import uuid
import platform

# External imports - conditional import for aws_lambda_powertools
try:
    from aws_lambda_powertools import Logger as PowertoolsLogger
    POWERTOOLS_AVAILABLE = True
except ImportError:
    PowertoolsLogger = None
    POWERTOOLS_AVAILABLE = False

class DataLakeLogger:
    """
    Clase centralizada para logging en DataLake que maneja automáticamente:
    - AWS CloudWatch (usando aws-lambda-powertools)
    - Archivos locales (.log)
    - Console output
    - Detección automática del entorno (AWS vs Local)
    """
    
    # Configuración global por defecto
    _global_config = {
        'log_level': logging.INFO,
        'log_directory': './logs',
        'service_name': 'datalake',
        'correlation_id': None,
        'owner': None,
        'auto_detect_env': True,
        'force_local_mode': False
    }
    
    # Cache de loggers para evitar recrear
    _logger_cache = {}
    
    @classmethod
    def configure_global(cls, 
                        log_level: Optional[int] = None,
                        log_directory: Optional[str] = None, 
                        service_name: Optional[str] = None,
                        correlation_id: Optional[str] = None,
                        owner: Optional[str] = None,
                        auto_detect_env: bool = True,
                        force_local_mode: bool = False):
        """
        Configura parámetros globales para todos los loggers
        
        Args:
            log_level: Nivel de logging (logging.DEBUG, logging.INFO, etc.)
            log_directory: Directorio donde guardar logs locales (default: ./logs)
            service_name: Nombre del servicio para todos los loggers
            correlation_id: ID de correlación para trazabilidad
            owner: Propietario del servicio
            auto_detect_env: Si debe detectar automáticamente el entorno
            force_local_mode: Forzar modo local (útil para testing)
        """
        if log_level is not None:
            cls._global_config['log_level'] = log_level
        if log_directory is not None:
            cls._global_config['log_directory'] = log_directory
        if service_name is not None:
            cls._global_config['service_name'] = service_name
        if correlation_id is not None:
            cls._global_config['correlation_id'] = correlation_id
        if owner is not None:
            cls._global_config['owner'] = owner
        
        cls._global_config['auto_detect_env'] = auto_detect_env
        cls._global_config['force_local_mode'] = force_local_mode
        
        # Limpiar cache cuando cambia configuración
        cls._logger_cache.clear()
        
        print(f"DataLakeLogger global config updated: {cls._global_config}")
    
    @classmethod
    def get_logger(cls, 
                   name: Optional[str] = None,
                   service_name: Optional[str] = None,
                   correlation_id: Optional[str] = None,
                   log_level: Optional[int] = None) -> logging.Logger:
        """
        Obtiene un logger configurado para el entorno actual
        
        Args:
            name: Nombre del logger (normalmente __name__)
            service_name: Nombre del servicio (override del global)
            correlation_id: ID de correlación (override del global)
            log_level: Nivel de log (override del global)
            
        Returns:
            Logger configurado para el entorno actual
        """
        
        # Usar configuración global como base
        effective_service = service_name or cls._global_config['service_name']
        effective_correlation_id = correlation_id or cls._global_config['correlation_id']
        effective_log_level = log_level or cls._global_config['log_level']
        
        # Crear cache key
        cache_key = f"{name}_{effective_service}_{effective_correlation_id}_{effective_log_level}"
        
        # Devolver del cache si existe
        if cache_key in cls._logger_cache:
            return cls._logger_cache[cache_key]
        
        # Detectar entorno
        is_aws = cls._is_aws_environment()
        
        if is_aws and not cls._global_config['force_local_mode']:
            logger = cls._create_aws_logger(name, effective_service, effective_correlation_id, effective_log_level)
        else:
            logger = cls._create_local_logger(name, effective_service, effective_log_level)
        
        # Guardar en cache
        cls._logger_cache[cache_key] = logger
        
        return logger
    
    @classmethod
    def _is_aws_environment(cls) -> bool:
        """Detecta si estamos ejecutando en AWS"""
        if not cls._global_config['auto_detect_env']:
            return False
            
        # Detectores de entorno AWS
        aws_indicators = [
            os.getenv('AWS_EXECUTION_ENV'),  # Lambda/Glue
            os.getenv('AWS_REGION'),
            os.getenv('AWS_DEFAULT_REGION'),
            os.getenv('GLUE_VERSION'),  # Específico de Glue
            'lambda' in os.getenv('AWS_EXECUTION_ENV', '').lower(),
            '/opt/ml' in os.getenv('SM_MODEL_DIR', ''),  # SageMaker
        ]
        
        return any(aws_indicators)
    
    @classmethod
    def _create_aws_logger(cls, name: str, service_name: str, correlation_id: str, log_level: int) -> logging.Logger:
        """Crea logger para entorno AWS usando PowerTools"""
        
        if not POWERTOOLS_AVAILABLE:
            print("WARNING: aws-lambda-powertools not available, falling back to local logger")
            return cls._create_local_logger(name, service_name, log_level)
        
        # Crear PowerTools logger
        powertools_logger = PowertoolsLogger(
            name=name or service_name,
            correlation_id=correlation_id,
            service=service_name,
            owner=cls._global_config['owner'],
            log_uncaught_exceptions=True,
            level=log_level
        )
        
        print(f"Created AWS PowerTools logger for: {name or service_name}")
        return powertools_logger
    
    @classmethod
    def _create_local_logger(cls, name: str, service_name: str, log_level: int) -> logging.Logger:
        """Crea logger para entorno local con archivo de log"""
        
        logger_name = name or service_name or 'datalake'
        
        # Crear logger estándar de Python
        logger = logging.getLogger(logger_name)
        logger.setLevel(log_level)
        
        # Limpiar handlers existentes para evitar duplicados
        if logger.handlers:
            logger.handlers.clear()
        
        # Crear formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Handler para consola
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # Handler para archivo (solo en modo local)
        log_file_path = cls._get_log_file_path(logger_name, service_name)
        if log_file_path:
            try:
                file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
                file_handler.setLevel(log_level)
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)
                print(f"Created local logger with file: {log_file_path}")
            except Exception as e:
                print(f"WARNING: Could not create log file {log_file_path}: {e}")
        
        # Evitar propagación para prevenir logs duplicados
        logger.propagate = False
        
        return logger
    
    @classmethod
    def _get_log_file_path(cls, logger_name: str, service_name: str) -> Optional[str]:
        """Genera ruta para archivo de log"""
        
        log_dir = cls._global_config['log_directory']
        
        # Crear directorio si no existe
        try:
            os.makedirs(log_dir, exist_ok=True)
        except Exception as e:
            print(f"WARNING: Could not create log directory {log_dir}: {e}")
            return None
        
        # Generar nombre de archivo con timestamp
        timestamp = dt.datetime.now().strftime('%Y%m%d')
        safe_logger_name = logger_name.replace('.', '_').replace(' ', '_')
        safe_service_name = service_name.replace('.', '_').replace(' ', '_')
        
        filename = f"{safe_service_name}_{safe_logger_name}_{timestamp}.log"
        return os.path.join(log_dir, filename)
    
    @classmethod
    def print_environment_info(cls):
        """Imprime información del entorno de logging detectado"""
        is_aws = cls._is_aws_environment()
        force_local = cls._global_config['force_local_mode']
        
        print("=" * 60)
        print("DATALAKE LOGGING ENVIRONMENT INFO")
        print("=" * 60)
        print(f"Platform: {platform.system()} {platform.release()}")
        print(f"Python: {platform.python_version()}")
        print(f"AWS Environment Detected: {is_aws}")
        print(f"PowerTools Available: {POWERTOOLS_AVAILABLE}")
        print(f"Force Local Mode: {force_local}")
        print(f"Log Level: {logging.getLevelName(cls._global_config['log_level'])}")
        print(f"Log Directory: {cls._global_config['log_directory']}")
        print(f"Service Name: {cls._global_config['service_name']}")
        
        # Mostrar variables de entorno AWS relevantes
        aws_vars = ['AWS_EXECUTION_ENV', 'AWS_REGION', 'GLUE_VERSION', 'AWS_DEFAULT_REGION']
        print("AWS Environment Variables:")
        for var in aws_vars:
            value = os.getenv(var, 'Not Set')
            print(f"  {var}: {value}")
        
        print("=" * 60)
    
    @classmethod
    def clear_cache(cls):
        """Limpia el cache de loggers (útil para testing)"""
        cls._logger_cache.clear()
        print("DataLakeLogger cache cleared")


# Funciones de conveniencia para compatibilidad hacia atrás
def get_logger(name: Optional[str] = None, **kwargs) -> logging.Logger:
    """
    Función de conveniencia para obtener un logger
    Reemplaza el custom_logger anterior
    """
    return DataLakeLogger.get_logger(name, **kwargs)

def configure_logging(**kwargs):
    """
    Función de conveniencia para configurar logging global
    Reemplaza set_logger_config anterior
    """
    DataLakeLogger.configure_global(**kwargs)

# Para compatibilidad hacia atrás
custom_logger = get_logger
set_logger_config = configure_logging