# Built-in imports
import os
import logging
from typing import Optional, Union
import uuid

# External imports - conditional import for aws_lambda_powertools
try:
    from aws_lambda_powertools import Logger as PowertoolsLogger
    POWERTOOLS_AVAILABLE = True
    print("SUCCESS: aws_lambda_powertools imported successfully in logger.py")
except ImportError as import_err:
    print(f"WARNING: aws_lambda_powertools import failed in logger.py: {import_err}")
    # Fallback for environments where aws_lambda_powertools is not available
    PowertoolsLogger = None
    POWERTOOLS_AVAILABLE = False

# Variables globales para la configuración
GLOBAL_LOG_FILE = None
GLOBAL_LOG_LEVEL = logging.INFO
GLOBAL_SERVICE = "service_undefined"
GLOBAL_CORRELATION_ID = None
GLOBAL_OWNER = None

def set_logger_config(
    log_level: Optional[int] = None, 
    log_file: Optional[str] = None,
    service: Optional[str] = None,
    correlation_id: Optional[Union[str, uuid.UUID, None]] = None,
    owner: Optional[str] = None
) -> None:
    """
    Establece la configuración global del logger que será usada por todas las instancias
    de custom_logger si no se especifican opciones a nivel individual.
    
    :param log_level: Nivel de logging (como logging.INFO, logging.DEBUG, etc.)
    :param log_file: Ruta al archivo de log. Si es None, no se escribirá a archivo
    :param service: Nombre del servicio para todos los loggers
    :param correlation_id: ID de correlación global para trazabilidad
    :param owner: Propietario del servicio
    """
    global GLOBAL_LOG_FILE, GLOBAL_LOG_LEVEL, GLOBAL_SERVICE, GLOBAL_CORRELATION_ID, GLOBAL_OWNER
    
    # Log file es opcional - puede ser None para desactivar el logging a archivo
    if log_file is not None:
        GLOBAL_LOG_FILE = log_file
        
        # Si log_file existe, aseguramos que el directorio existe
        if log_file:
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)
    
    if log_level is not None:
        GLOBAL_LOG_LEVEL = log_level
        
    if service is not None:
        GLOBAL_SERVICE = service
        
    if correlation_id is not None:
        GLOBAL_CORRELATION_ID = correlation_id
        
    if owner is not None:
        GLOBAL_OWNER = owner

def custom_logger(
    name: Optional[str] = None,
    correlation_id: Optional[Union[str, uuid.UUID, None]] = None,
    service: Optional[str] = None,
    owner: Optional[str] = None,
    log_level: Optional[int] = None,
    log_file: Optional[str] = None,
):
    """
    Returns a custom Logger Object with optional file logging capability.
    
    :param name: Logger name
    :param correlation_id: Correlation ID for tracing (si es None, usa el global)
    :param service: Service name (si es None, usa el global)
    :param owner: Owner name (si es None, usa el global)
    :param log_file: Optional path to log file. If None, uses global setting (which could also be None)
    :param log_level: Logging level for file logger (if None, uses global setting)
    :return: aws_lambda_powertools.Logger object or standard logging.Logger fallback
    """
    # Usar configuración global si no se proporcionan parámetros específicos
    effective_log_level = log_level if log_level is not None else GLOBAL_LOG_LEVEL
    effective_log_file = log_file if log_file is not None else GLOBAL_LOG_FILE
    effective_service = service if service is not None else GLOBAL_SERVICE
    effective_correlation_id = correlation_id if correlation_id is not None else GLOBAL_CORRELATION_ID
    effective_owner = owner if owner is not None else GLOBAL_OWNER
    
    if POWERTOOLS_AVAILABLE:
        # Create the standard powertools logger
        powertools_logger = PowertoolsLogger(
            name=name,
            correlation_id=effective_correlation_id,
            service=effective_service,
            owner=effective_owner,
            log_uncaught_exceptions=True,
        )
        
        # If effective_log_file is set, only then configure file logging
        if effective_log_file:
            # Create a standard Python logger for file logging
            file_logger = logging.getLogger(f"{name}_file") if name else logging.getLogger("file_logger")
            file_logger.setLevel(effective_log_level)
            
            # Clear existing handlers to avoid duplicates
            if file_logger.handlers:
                file_logger.handlers = []
                
            # Create file handler
            file_handler = logging.FileHandler(effective_log_file)
            file_handler.setLevel(effective_log_level)
            
            # Create formatter for file logging
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(formatter)
            file_logger.addHandler(file_handler)
            
            # Attach file logger to powertools logger for file logging
            powertools_logger._file_logger = file_logger
            
            # Override logging methods to also log to file
            original_info = powertools_logger.info
            original_error = powertools_logger.error
            original_warning = powertools_logger.warning
            original_debug = powertools_logger.debug
            
            def info_with_file(message, **kwargs):
                file_logger.info(message)
                return original_info(message, **kwargs)
            
            def error_with_file(message, **kwargs):
                file_logger.error(message)
                return original_error(message, **kwargs)
            
            def warning_with_file(message, **kwargs):
                file_logger.warning(message)
                return original_warning(message, **kwargs)
            
            def debug_with_file(message, **kwargs):
                file_logger.debug(message)
                return original_debug(message, **kwargs)
            
            powertools_logger.info = info_with_file
            powertools_logger.error = error_with_file
            powertools_logger.warning = warning_with_file
            powertools_logger.debug = debug_with_file
        
        return powertools_logger
    else:
        # Fallback to standard Python logger when powertools is not available
        print(f"Using fallback standard logger for: {name or 'default'}")
        fallback_logger = logging.getLogger(name or 'aje_libs_fallback')
        fallback_logger.setLevel(effective_log_level)
        
        # Clear existing handlers to avoid duplicates
        if fallback_logger.handlers:
            fallback_logger.handlers = []
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(effective_log_level)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        fallback_logger.addHandler(console_handler)
        
        # Add file handler if specified
        if effective_log_file:
            file_handler = logging.FileHandler(effective_log_file)
            file_handler.setLevel(effective_log_level)
            file_handler.setFormatter(formatter)
            fallback_logger.addHandler(file_handler)
        
        return fallback_logger