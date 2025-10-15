# strategies/strategy_factory_v2.py
from typing import Optional
from .base.extraction_strategy import ExtractionStrategy
from .base.strategy_types import ExtractionStrategyType
from .registry.strategy_registry import StrategyRegistry
from interfaces.strategy_interface import StrategyInterface
from interfaces.watermark_interface import WatermarkStorageInterface
from models.table_config import TableConfig
from models.extraction_config import ExtractionConfig
from exceptions.custom_exceptions import ConfigurationError
from aje_libs.common.datalake_logger import DataLakeLogger

logger = DataLakeLogger.get_logger(__name__)

class StrategyFactory:
    """Factory simplificado para crear estrategias de extracción"""
    
    @classmethod
    def create(cls, table_config: TableConfig, extraction_config: ExtractionConfig,
               watermark_storage: WatermarkStorageInterface = None) -> StrategyInterface:
        """Crea la estrategia apropiada basada en configuración"""
        
        logger.info(f"=== STRATEGY FACTORY ===")
        logger.info(f"Table: {extraction_config.table_name}")
        
        # Determinar tipo de estrategia
        strategy_type = cls._determine_strategy_type(table_config, extraction_config)
        logger.info(f"Determined strategy type: {strategy_type.value}")
        
        # Verificar que esté registrada
        if not StrategyRegistry.is_registered(strategy_type):
            available = [st.value for st in StrategyRegistry.get_available_strategies()]
            error_msg = f"Strategy type '{strategy_type.value}' not registered. Available: {available}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg)
        
        # Crear instancia de la nueva estrategia
        strategy_class = StrategyRegistry.get_strategy_class(strategy_type)
        
        logger.info(f"Creating strategy instance: {strategy_class.__name__}")
        
        new_strategy = strategy_class(table_config, extraction_config, watermark_storage)
        
        # Validar configuración
        if not new_strategy.validate_and_cache():
            error_msg = f"Strategy validation failed for {strategy_class.__name__}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg)
        
        # Envolver en adaptador para compatibilidad
        from .adapters.strategy_adapter import StrategyAdapter
        strategy_adapter = StrategyAdapter(new_strategy)
        
        logger.info(f"Strategy instance created and wrapped in adapter successfully")
        logger.info("=== END STRATEGY FACTORY ===")
        
        return strategy_adapter
    
    @classmethod
    def _determine_strategy_type(cls, table_config: TableConfig, 
                            extraction_config: ExtractionConfig) -> ExtractionStrategyType:
        """Determina qué estrategia usar basada en configuración"""
        
        # Force full load override
        if extraction_config.force_full_load:
            logger.info("Force full load enabled - using FULL_LOAD strategy")
            return ExtractionStrategyType.FULL_LOAD
        
        # Usar load_type de configuración
        load_type = table_config.load_type.lower().strip() if table_config.load_type else 'full'
        logger.info(f"Load type from config: '{load_type}'")
        
        # Lógica específica para determinar estrategia
        if load_type in ['incremental']:
            # Verificar si tiene los campos necesarios para incremental
            has_incremental_config = (
                (hasattr(table_config, 'filter_column') and table_config.filter_column) or
                (hasattr(table_config, 'partition_column') and table_config.partition_column)
            )
            
            if has_incremental_config:
                logger.info("Detected incremental configuration - using INCREMENTAL strategy")
                return ExtractionStrategyType.INCREMENTAL
            else:
                logger.warning("Incremental load_type but missing config - falling back to FULL_LOAD")
                return ExtractionStrategyType.FULL_LOAD
        
        elif load_type in ['date_range', 'between-date', 'time_range']:
            # Verificar si tiene configuración para time range
            has_time_range_config = (
                hasattr(table_config, 'filter_column') and table_config.filter_column and
                ((hasattr(table_config, 'start_value') and table_config.start_value and
                hasattr(table_config, 'end_value') and table_config.end_value) or
                (hasattr(table_config, 'delay_incremental_ini') and table_config.delay_incremental_ini))
            )
            
            if has_time_range_config:
                logger.info(f"Detected time range configuration for '{load_type}' - using TIME_RANGE strategy")
                return ExtractionStrategyType.TIME_RANGE
            else:
                logger.warning(f"Time range load_type '{load_type}' but missing config - falling back to FULL_LOAD")
                return ExtractionStrategyType.FULL_LOAD
        
        # Default a full load
        try:
            strategy_type = ExtractionStrategyType.from_string(load_type)
            logger.info(f"Mapped to strategy type: {strategy_type.value}")
            return strategy_type
        except ValueError as e:
            logger.warning(f"Could not map load_type '{load_type}': {e}")
            logger.info("Defaulting to FULL_LOAD strategy")
            return ExtractionStrategyType.FULL_LOAD
    
    @classmethod
    def get_supported_strategies(cls) -> list:
        """Obtiene lista de estrategias soportadas"""
        return [st.value for st in StrategyRegistry.get_available_strategies()]
    
    @classmethod
    def validate_partition_mode(cls, table_config: TableConfig, extraction_config: ExtractionConfig):
        """🆕 NUEVO MÉTODO: Valida la configuración de PARTITION_MODE"""
        
        partition_mode = getattr(table_config, 'partition_mode', 'AUTO').upper()
        
        # Validar que PARTITION_MODE solo se use con FULL o TIME_RANGE
        if partition_mode != 'AUTO' and table_config.load_type not in ['full', 'time_range']:
            logger.warning(f"PARTITION_MODE={partition_mode} ignored for load_type={table_config.load_type}")
        
        # Validar que MIN_MAX tenga PARTITION_COLUMN
        if partition_mode == 'MIN_MAX':
            if not (hasattr(table_config, 'partition_column') and table_config.partition_column):
                raise ValueError(f"PARTITION_MODE=MIN_MAX requires PARTITION_COLUMN to be configured")
        
        logger.info(f"✅ PARTITION_MODE validation passed: {partition_mode}")