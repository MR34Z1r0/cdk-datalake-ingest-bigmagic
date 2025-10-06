# strategies/implementations/time_range.py

from typing import List
from ..base.extraction_strategy import ExtractionStrategy
from ..base.extraction_params import ExtractionParams
from ..base.strategy_types import ExtractionStrategyType
from aje_libs.common.datalake_logger import DataLakeLogger
from models.load_mode import LoadMode

logger = DataLakeLogger.get_logger(__name__)

class TimeRangeStrategy(ExtractionStrategy):
    """Estrategia para carga por rango de fechas específico"""
    
    def get_strategy_type(self) -> ExtractionStrategyType:
        return ExtractionStrategyType.TIME_RANGE
    
    def build_extraction_params(self) -> ExtractionParams:
        """Construye parámetros para carga por rango de tiempo"""
        logger.info(f"=== TIME RANGE STRATEGY - Building Params ===")
        logger.info(f"Table: {self.extraction_config.table_name}")
        logger.info(f"Load Mode: {self.extraction_config.load_mode.value}")
        
        # TIME RANGE NO USA WATERMARKS
        if self.watermark_storage:
            logger.info("⚠️ Watermark storage provided but not used in time range strategy")
        
        # MODO INITIAL: Carga histórica completa
        if self.extraction_config.load_mode == LoadMode.INITIAL:
            logger.info("🆕 INITIAL mode detected - Loading ALL historical data")
            return self._build_initial_load_params()
        
        # Modo NORMAL: Detectar si necesita particionado
        if self._should_use_partitioned_load():
            logger.info("⚠️ Partitioned time range load detected - will be handled by orchestrator")
            return self._build_partitioned_params()
        
        # Carga no particionada (normal)
        logger.info("📋 Building non-partitioned time range params")
        return self._build_non_partitioned_params()
    
    def _should_use_partitioned_load(self) -> bool:
        """Detecta si la tabla requiere particionado"""
        return (
            hasattr(self.table_config, 'source_table_type') and 
            self.table_config.source_table_type == 't' and
            hasattr(self.table_config, 'partition_column') and 
            self.table_config.partition_column and 
            self.table_config.partition_column.strip() != ''
        )
    
    def _build_initial_load_params(self) -> ExtractionParams:
        """
        Construye parámetros para CARGA INICIAL (modo INITIAL)
        
        Para tablas transaccionales: usa MIN/MAX sin filtros de fecha
        Para tablas maestras: carga completa sin filtros de fecha
        """
        logger.info("🔧 Building INITIAL load params (historical data)")
        
        # Detectar si es tabla transaccional
        is_transactional = self._should_use_partitioned_load()
        
        if is_transactional:
            logger.info("📊 Transactional table detected - will use MIN/MAX partitioning")
            return self._build_partitioned_initial_params()
        else:
            logger.info("📋 Master table - full load without date filters")
            return self._build_non_partitioned_initial_params()
    
    def _build_partitioned_initial_params(self) -> ExtractionParams:
        """
        Construye parámetros para carga inicial PARTICIONADA
        
        Carga TODO desde el inicio usando MIN/MAX, SIN filtros de fecha
        """
        logger.info("🔧 Building PARTITIONED INITIAL load params")
        
        # Construir table_name con JOIN
        table_name_with_joins = f"{self.table_config.source_schema}.{self.table_config.source_table}"
        
        if hasattr(self.table_config, 'join_expr') and self.table_config.join_expr and self.table_config.join_expr.strip():
            table_name_with_joins += f" {self.table_config.join_expr.strip()}"
            logger.info(f"📎 Table with JOIN: {table_name_with_joins}")
        
        # Construir metadata para particionado
        metadata = {
            **self._build_basic_metadata(),
            'needs_partitioning': True,
            'partition_column': self.table_config.partition_column,
            'source_table_type': self.table_config.source_table_type,
            'chunk_size': self.extraction_config.chunk_size,
            'strategy_type': 'time_range',
            'load_mode': 'initial'
        }
        
        # Crear params
        params = ExtractionParams(
            table_name=table_name_with_joins,
            columns=self._parse_columns(),
            metadata=metadata
        )
        
        # SOLO filtros básicos (FILTER_EXP), SIN filtros de fecha
        basic_filters = self._build_basic_filters()
        for filter_condition in basic_filters:
            if filter_condition:
                params.add_where_condition(filter_condition)
                logger.info(f"➕ Basic filter: {filter_condition}")
        
        logger.info(f"✅ Partitioned INITIAL params built - will load ALL historical data")
        logger.info(f"   Partition column: {self.table_config.partition_column}")
        logger.info(f"   Total filters: {len(params.where_conditions)} (NO date filters)")
        
        return params
    
    def _build_non_partitioned_initial_params(self) -> ExtractionParams:
        """
        Construye parámetros para carga inicial NO particionada
        
        Carga TODO desde el inicio, SIN filtros de fecha
        """
        logger.info("📋 Building NON-PARTITIONED INITIAL load params")
        
        # Construir table_name con JOIN
        table_name_with_joins = f"{self.table_config.source_schema}.{self.table_config.source_table}"
        
        if hasattr(self.table_config, 'join_expr') and self.table_config.join_expr and self.table_config.join_expr.strip():
            table_name_with_joins += f" {self.table_config.join_expr.strip()}"
        
        # Crear parámetros básicos
        params = ExtractionParams(
            table_name=table_name_with_joins,
            columns=self._parse_columns(),
            metadata=self._build_basic_metadata()
        )
        
        # SOLO filtros básicos (FILTER_EXP), SIN filtros de fecha
        basic_filters = self._build_basic_filters()
        for filter_condition in basic_filters:
            if filter_condition:
                params.add_where_condition(filter_condition)
                logger.info(f"➕ Basic filter: {filter_condition}")
        
        logger.info(f"✅ Non-partitioned INITIAL params built - will load ALL historical data")
        logger.info(f"   Total filters: {len(params.where_conditions)} (NO date filters)")
        
        return params
    
    def _build_partitioned_params(self) -> ExtractionParams:
        """
        Construye parámetros para carga particionada con time range NORMAL
        """
        logger.info("🔧 Building PARTITIONED params for time range load")
        
        table_name_with_joins = f"{self.table_config.source_schema}.{self.table_config.source_table}"
        
        if hasattr(self.table_config, 'join_expr') and self.table_config.join_expr and self.table_config.join_expr.strip():
            table_name_with_joins += f" {self.table_config.join_expr.strip()}"
        
        metadata = {
            **self._build_basic_metadata(),
            'needs_partitioning': True,
            'partition_column': self.table_config.partition_column,
            'source_table_type': self.table_config.source_table_type,
            'chunk_size': self.extraction_config.chunk_size,
            'strategy_type': 'time_range'
        }
        
        params = ExtractionParams(
            table_name=table_name_with_joins,
            columns=self._parse_columns(),
            metadata=metadata
        )
        
        # Agregar filtros de TIME RANGE
        time_range_filters = self._build_time_range_filters()
        for filter_condition in time_range_filters:
            if filter_condition:
                params.add_where_condition(filter_condition)
        
        # Agregar filtros básicos adicionales
        basic_filters = self._build_basic_filters()
        for filter_condition in basic_filters:
            if filter_condition:
                params.add_where_condition(filter_condition)
        
        return params
    
    def _build_non_partitioned_params(self) -> ExtractionParams:
        """Construye parámetros para carga time range NO particionada"""
        
        table_name_with_joins = f"{self.table_config.source_schema}.{self.table_config.source_table}"
        
        if hasattr(self.table_config, 'join_expr') and self.table_config.join_expr and self.table_config.join_expr.strip():
            table_name_with_joins += f" {self.table_config.join_expr.strip()}"
        
        params = ExtractionParams(
            table_name=table_name_with_joins,
            columns=self._parse_columns(),
            metadata=self._build_basic_metadata()
        )
        
        # Agregar filtros de time range
        time_range_filters = self._build_time_range_filters()
        for filter_condition in time_range_filters:
            if filter_condition:
                params.add_where_condition(filter_condition)
        
        # Agregar filtros básicos
        basic_filters = self._build_basic_filters()
        for filter_condition in basic_filters:
            if filter_condition:
                params.add_where_condition(filter_condition)
        
        return params
    
    def _build_time_range_filters(self) -> List[str]:
        """Construye los filtros de rango de tiempo (explícito o dinámico)"""
        filters = []
        
        has_explicit_range = (
            hasattr(self.table_config, 'start_value') and self.table_config.start_value and
            hasattr(self.table_config, 'end_value') and self.table_config.end_value and
            str(self.table_config.start_value).strip() and str(self.table_config.end_value).strip()
        )
        
        has_dynamic_range = (
            hasattr(self.table_config, 'delay_incremental_ini') and 
            self.table_config.delay_incremental_ini and
            str(self.table_config.delay_incremental_ini).strip()
        )
        
        if has_explicit_range:
            explicit_range_filter = self._build_explicit_time_range_filter()
            if explicit_range_filter:
                filters.append(explicit_range_filter)
        elif has_dynamic_range:
            date_range_filter = self._build_date_range_filter()
            if date_range_filter:
                filters.append(date_range_filter)
        
        return filters
    
    def _build_explicit_time_range_filter(self) -> str:
        """Construye filtro con valores explícitos de start y end"""
        try:
            filter_column = self.table_config.filter_column.strip()
            start_value = str(self.table_config.start_value).strip()
            end_value = str(self.table_config.end_value).strip()
            
            data_type = getattr(self.table_config, 'filter_data_type', '').lower()
            
            if 'date' in data_type or 'time' in data_type or 'char' in data_type:
                filter_condition = f"{filter_column} BETWEEN '{start_value}' AND '{end_value}'"
            else:
                filter_condition = f"{filter_column} BETWEEN {start_value} AND {end_value}"
            
            logger.info(f"Built explicit time range filter: {filter_condition}")
            return filter_condition
            
        except Exception as e:
            logger.warning(f"Failed to build explicit time range filter: {e}")
            return None
    
    def _build_date_range_filter(self) -> str:
        """Construye filtro usando delay_incremental_ini y opcionalmente delay_incremental_end"""
        try:
            has_delay_end = (
                hasattr(self.table_config, 'delay_incremental_end') and 
                self.table_config.delay_incremental_end and
                str(self.table_config.delay_incremental_end).strip()
            )
            
            if has_delay_end:
                from utils.date_utils import get_date_limits_with_range
                
                clean_delay_ini = self.table_config.delay_incremental_ini.strip().replace("'", "")
                clean_delay_end = self.table_config.delay_incremental_end.strip().replace("'", "")
                
                logger.info(f"Using delay range: INI={clean_delay_ini}, END={clean_delay_end}")
                
                lower_limit, upper_limit = get_date_limits_with_range(
                    clean_delay_ini,
                    clean_delay_end,
                    getattr(self.table_config, 'filter_data_type', '') or ""
                )
            else:
                from utils.date_utils import get_date_limits
                
                clean_delay_ini = self.table_config.delay_incremental_ini.strip().replace("'", "")
                
                lower_limit, upper_limit = get_date_limits(
                    clean_delay_ini,
                    getattr(self.table_config, 'filter_data_type', '') or ""
                )
            
            filter_condition = self.table_config.filter_column.replace(
                '{0}', lower_limit
            ).replace(
                '{1}', upper_limit
            ).replace('"', '')
            
            logger.info(f"Built date range filter: {filter_condition}")
            return filter_condition
            
        except Exception as e:
            logger.warning(f"Failed to build date range filter: {e}")
            return None
    
    def _build_basic_filters(self) -> List[str]:
        """Construye SOLO filtros básicos (FILTER_EXP)"""
        filters = []
        
        if hasattr(self.table_config, 'filter_exp') and self.table_config.filter_exp:
            clean_filter = self.table_config.filter_exp.replace('"', '').strip()
            if clean_filter:
                filters.append(clean_filter)
        
        return filters
    
    def validate(self) -> bool:
        """Valida configuración para time range"""
        logger.info("=== TIME RANGE STRATEGY VALIDATION ===")
        
        required_fields = [
            ('stage_table_name', self.table_config.stage_table_name),
            ('source_schema', self.table_config.source_schema),
            ('source_table', self.table_config.source_table),
            ('columns', self.table_config.columns)
        ]
        
        validation_errors = []
        for field_name, field_value in required_fields:
            logger.info(f"Checking {field_name}: '{field_value}'")
            
            if field_value is None:
                validation_errors.append(f"{field_name} is None")
            elif not str(field_value).strip():
                validation_errors.append(f"{field_name} is empty")
            else:
                logger.info(f"  ✅ {field_name} is valid")
        
        if validation_errors:
            logger.error("❌ VALIDATION FAILED:")
            for error in validation_errors:
                logger.error(f"  - {error}")
            return False
        
        logger.info("✅ ALL VALIDATION CHECKS PASSED")
        logger.info("=== END VALIDATION ===")
        return True