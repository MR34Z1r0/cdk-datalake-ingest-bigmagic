# Built-in imports
import os
import logging
from typing import Optional, Union
import uuid

# External imports
from aws_lambda_powertools import Logger

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
) -> Logger:
    """
    Returns a custom Logger Object with optional file logging capability.
    
    :param name: Logger name
    :param correlation_id: Correlation ID for tracing (si es None, usa el global)
    :param service: Service name (si es None, usa el global)
    :param owner: Owner name (si es None, usa el global)
    :param log_file: Optional path to log file. If None, uses global setting (which could also be None)
    :param log_level: Logging level for file logger (if None, uses global setting)
    :return: aws_lambda_powertools.Logger object
    """
    # Usar configuración global si no se proporcionan parámetros específicos
    effective_log_level = log_level if log_level is not None else GLOBAL_LOG_LEVEL
    effective_log_file = log_file if log_file is not None else GLOBAL_LOG_FILE
    effective_service = service if service is not None else GLOBAL_SERVICE
    effective_correlation_id = correlation_id if correlation_id is not None else GLOBAL_CORRELATION_ID
    effective_owner = owner if owner is not None else GLOBAL_OWNER
    
    # Create the standard powertools logger
    powertools_logger = Logger(
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
        
        # Create directory if it doesn't exist
        log_dir = os.path.dirname(effective_log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        # Add file handler
        file_handler = logging.FileHandler(effective_log_file)
        file_handler.setLevel(effective_log_level)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        file_logger.addHandler(file_handler)
        
        # Monkey patch the powertools_logger to also log to file
        original_info = powertools_logger.info
        original_error = powertools_logger.error
        original_warning = powertools_logger.warning
        original_debug = powertools_logger.debug
        
        def patched_info(msg, *args, **kwargs):
            original_info(msg, *args, **kwargs)
            file_logger.info(msg)
            
        def patched_error(msg, *args, **kwargs):
            original_error(msg, *args, **kwargs)
            file_logger.error(msg)
            
        def patched_warning(msg, *args, **kwargs):
            original_warning(msg, *args, **kwargs)
            file_logger.warning(msg)
            
        def patched_debug(msg, *args, **kwargs):
            original_debug(msg, *args, **kwargs)
            file_logger.debug(msg)
        
        # Replace the methods
        powertools_logger.info = patched_info
        powertools_logger.error = patched_error
        powertools_logger.warning = patched_warning
        powertools_logger.debug = patched_debug
    
    return powertools_logger