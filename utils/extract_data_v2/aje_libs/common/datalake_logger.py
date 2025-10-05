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
    - Archivos locales (.log) - CONSOLIDADO en UN SOLO archivo
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
    
    # Cache de file handlers para reutilizar el MISMO archivo
    _file_handler_cache = {}
    
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
        cls._file_handler_cache.clear()
        
        print(f"DataLakeLogger global config updated: {cls._global_config}")
    
    @classmethod
    def get_logger(cls, 
                   name: Optional[str] = None,
                   service_name: Optional[str] = None,
                   correlation_id: Optional[str] = None,
                   log_level: Optional[int] = None) -> logging.Logger:
        """
        Obtiene un logger configurado para el entorno actual
        
        🔧 CONSOLIDADO: Todos los loggers escriben al MISMO archivo por día
        
        Args:
            name: Nombre del logger (normalmente __name__)
            service_name: Nombre del servicio (IGNORADO - usa global)
            correlation_id: ID de correlación (opcional)
            log_level: Nivel de log (opcional)
            
        Returns:
            Logger configurado
        """
        
        # Usar configuración global
        final_log_level = log_level or cls._global_config['log_level']
        
        # 🔑 CLAVE: Usar SIEMPRE el service_name GLOBAL
        # Esto fuerza que TODOS escriban al mismo archivo
        final_service_name = cls._global_config['service_name']
        
        final_correlation_id = correlation_id or cls._global_config.get('correlation_id')
        
        # Generar nombre del logger
        logger_name = name or __name__
        
        # 🔑 Cache key simplificado - usar solo logger_name
        cache_key = logger_name
        
        # Verificar si ya existe en cache
        if cache_key in cls._logger_cache:
            return cls._logger_cache[cache_key]
        
        # Detectar si está en AWS
        is_aws = cls._is_aws_environment() and not cls._global_config['force_local_mode']
        
        if is_aws and POWERTOOLS_AVAILABLE:
            # Modo AWS con PowerTools
            logger = cls._create_powertools_logger(
                logger_name, 
                final_service_name, 
                final_log_level,
                final_correlation_id
            )
        else:
            # Modo Local - UN SOLO ARCHIVO
            logger = cls._create_local_logger(
                logger_name, 
                final_service_name,
                final_log_level
            )
        
        # Guardar en cache
        cls._logger_cache[cache_key] = logger
        
        return logger
    
    @classmethod
    def _is_aws_environment(cls) -> bool:
        """Detecta si está corriendo en AWS"""
        aws_indicators = [
            'AWS_EXECUTION_ENV',
            'AWS_LAMBDA_FUNCTION_NAME',
            'GLUE_VERSION',
            'AWS_REGION'
        ]
        
        return any(os.getenv(indicator) for indicator in aws_indicators)
    
    @classmethod
    def _create_powertools_logger(cls, 
                                  logger_name: str, 
                                  service_name: str, 
                                  log_level: int,
                                  correlation_id: Optional[str] = None) -> logging.Logger:
        """Crea logger usando AWS Lambda Powertools (para CloudWatch)"""
        
        if not POWERTOOLS_AVAILABLE:
            raise ImportError("aws-lambda-powertools not available")
        
        logger = PowertoolsLogger(
            service=service_name,
            level=logging.getLevelName(log_level),
            stream=None,
            logger_handler=None
        )
        
        if correlation_id:
            logger.append_keys(correlation_id=correlation_id)
        
        if cls._global_config.get('owner'):
            logger.append_keys(owner=cls._global_config['owner'])
        
        return logger
    
    @classmethod
    def _create_local_logger(cls, logger_name: str, service_name: str, log_level: int) -> logging.Logger:
        """
        Crea logger local que escribe a archivo y consola
        
        🔧 CONSOLIDADO: TODOS escriben al MISMO archivo
        """
        
        # Crear logger con nombre jerárquico
        logger = logging.getLogger(logger_name)
        logger.setLevel(log_level)
        
        # Limpiar handlers existentes para este logger específico
        logger.handlers.clear()
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Handler para consola (individual por logger)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # 🔑 Handler para archivo (COMPARTIDO por TODOS)
        log_file_path = cls._get_log_file_path(service_name)
        if log_file_path:
            # Verificar si ya existe un file handler para este archivo
            if log_file_path not in cls._file_handler_cache:
                try:
                    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
                    file_handler.setLevel(log_level)
                    file_handler.setFormatter(formatter)
                    cls._file_handler_cache[log_file_path] = file_handler
                    print(f"📝 Created log file: {log_file_path}")
                except Exception as e:
                    print(f"WARNING: Could not create log file {log_file_path}: {e}")
                    logger.addHandler(console_handler)
                    logger.propagate = False
                    return logger
            
            # Reutilizar el file handler existente
            file_handler = cls._file_handler_cache[log_file_path]
            logger.addHandler(file_handler)
        
        # Evitar propagación para prevenir logs duplicados
        logger.propagate = False
        
        return logger
    
    @classmethod
    def _get_log_file_path(cls, service_name: str) -> Optional[str]:
        """
        Genera ruta para archivo de log
        
        🔧 UN SOLO archivo por service_name y fecha
        """
        
        log_dir = cls._global_config['log_directory']
        
        # Crear directorio si no existe
        try:
            os.makedirs(log_dir, exist_ok=True)
        except Exception as e:
            print(f"WARNING: Could not create log directory {log_dir}: {e}")
            return None
        
        # 🔑 UN SOLO archivo usando SOLO service_name
        timestamp = dt.datetime.now().strftime('%Y%m%d')
        safe_service_name = service_name.replace('.', '_').replace(' ', '_')
        
        # Formato: {service_name}_{date}.log
        # TODOS los módulos escriben aquí
        filename = f"{safe_service_name}_{timestamp}.log"
        
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
        cls._file_handler_cache.clear()
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