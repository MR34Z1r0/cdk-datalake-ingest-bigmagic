# strategies/implementations/time_range.py
from typing import List
from ..base.extraction_strategy import ExtractionStrategy
from ..base.extraction_params import ExtractionParams
from ..base.strategy_types import ExtractionStrategyType
from aje_libs.common.datalake_logger import DataLakeLogger

logger = DataLakeLogger.get_logger(__name__)

class TimeRangeStrategy(ExtractionStrategy):
    """Estrategia para carga por rango de fechas específico"""
    
    def build_extraction_params(self) -> ExtractionParams:
        """Construye parámetros para carga por rango de tiempo"""
        logger.info(f"=== TIME RANGE STRATEGY - Building Params ===")
        logger.info(f"Table: {self.extraction_config.table_name}")
        logger.info(f"Load Mode: {self.extraction_config.load_mode.value}")
        
        # TIME RANGE NO USA WATERMARKS
        if self.watermark_storage:
            logger.info("⚠️ Watermark storage provided but not used in time range strategy")
        
        # 🆕 MODO INITIAL: Carga histórica completa
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
            'load_mode': 'initial'  # Marcar como initial
        }
        
        # Crear params
        params = ExtractionParams(
            table_name=table_name_with_joins,
            columns=self._parse_columns(),
            metadata=metadata
        )
        
        # 🔑 SOLO filtros básicos (FILTER_EXP), SIN filtros de fecha
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
        
        # 🔑 SOLO filtros básicos (FILTER_EXP), SIN filtros de fecha
        basic_filters = self._build_basic_filters()
        for filter_condition in basic_filters:
            if filter_condition:
                params.add_where_condition(filter_condition)
                logger.info(f"➕ Basic filter: {filter_condition}")
        
        logger.info(f"✅ Non-partitioned INITIAL params built - will load ALL historical data")
        logger.info(f"   Total filters: {len(params.where_conditions)} (NO date filters)")
        
        return params

    def _build_basic_filters(self) -> List[str]:
        """Construye SOLO filtros básicos (FILTER_EXP), sin filtros de fecha"""
        filters = []
        
        # SOLO filtro básico de la configuración (FILTER_EXP)
        if hasattr(self.table_config, 'filter_exp') and self.table_config.filter_exp:
            clean_filter = self.table_config.filter_exp.replace('"', '').strip()
            if clean_filter:
                filters.append(clean_filter)
                logger.info(f"Added basic filter from FILTER_EXP")
        
        return filters

    def get_strategy_type(self) -> ExtractionStrategyType:
        return ExtractionStrategyType.TIME_RANGE
    
    def validate(self) -> bool:
        """Valida configuración para carga por rango de tiempo"""
        logger.info("=== TIME RANGE STRATEGY VALIDATION ===")
        
        # Campos requeridos básicos
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
        
        # Validaciones específicas para time range
        time_range_errors = self._validate_time_range_config()
        validation_errors.extend(time_range_errors)
        
        if validation_errors:
            for error in validation_errors:
                logger.error(f"  ❌ {error}")
            logger.error("=== VALIDATION FAILED ===")
            return False
        
        logger.info("✅ ALL VALIDATION CHECKS PASSED")
        logger.info("=== END VALIDATION ===")
        return True

    def _build_time_range_filters(self) -> List[str]:
        """Construye los filtros de rango de tiempo (explícito o dinámico)"""
        filters = []
        
        # Verificar configuraciones de rango disponibles
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
        
        # PRIORIDAD 1: Rango explícito (START_VALUE y END_VALUE)
        if has_explicit_range:
            logger.info("🎯 Detected EXPLICIT range configuration (using START_VALUE and END_VALUE)")
            try:
                explicit_range_filter = self._build_explicit_time_range_filter()
                if explicit_range_filter:
                    filters.append(explicit_range_filter)
                    logger.info(f"✅ Applied explicit range filter: {explicit_range_filter}")
            except Exception as e:
                logger.warning(f"❌ Could not build explicit time range filter: {e}")
        
        # PRIORIDAD 2: Rango dinámico (DELAY_INCREMENTAL_INI)
        elif has_dynamic_range:
            logger.info("📅 Detected DYNAMIC range configuration (using DELAY_INCREMENTAL_INI)")
            try:
                date_range_filter = self._build_date_range_filter()
                if date_range_filter:
                    filters.append(date_range_filter)
                    logger.info(f"✅ Applied dynamic range filter: {date_range_filter}")
            except Exception as e:
                logger.warning(f"❌ Could not build dynamic date range filter: {e}")
        
        else:
            logger.error("❌ No valid range configuration found! Need either (START_VALUE + END_VALUE) or DELAY_INCREMENTAL_INI")
        
        return filters

    def _build_date_range_filter(self) -> str:
        """
        Construye filtro de rango de fechas usando delay_incremental_ini 
        y opcionalmente delay_incremental_end
        """
        try:
            # 🔑 Verificar si existe delay_incremental_end
            has_delay_end = (
                hasattr(self.table_config, 'delay_incremental_end') and 
                self.table_config.delay_incremental_end and
                str(self.table_config.delay_incremental_end).strip()
            )
            
            if has_delay_end:
                # 🆕 Usar función que acepta AMBOS parámetros
                from utils.date_utils import get_date_limits_with_range
                
                clean_delay_ini = self.table_config.delay_incremental_ini.strip().replace("'", "")
                clean_delay_end = self.table_config.delay_incremental_end.strip().replace("'", "")
                
                logger.info(f"📅 Date range with both delays: INI={clean_delay_ini}, END={clean_delay_end}")
                
                # Obtener límites con AMBOS delays
                lower_limit, upper_limit = get_date_limits_with_range(
                    clean_delay_ini,
                    clean_delay_end,
                    getattr(self.table_config, 'filter_data_type', '') or ""
                )
            else:
                # ✅ Comportamiento original: solo delay_incremental_ini
                from utils.date_utils import get_date_limits
                
                clean_delay_ini = self.table_config.delay_incremental_ini.strip().replace("'", "")
                
                logger.info(f"📅 Date range with single delay: INI={clean_delay_ini} (END defaults to today)")
                
                # Obtener límites (upper_limit será "hoy")
                lower_limit, upper_limit = get_date_limits(
                    clean_delay_ini,
                    getattr(self.table_config, 'filter_data_type', '') or ""
                )
            
            # Construir condición de filtro de rango
            filter_condition = self.table_config.filter_column.replace(
                '{0}', lower_limit
            ).replace(
                '{1}', upper_limit
            ).replace('"', '')
            
            logger.info(f"✅ Built date range filter: {filter_condition}")
            logger.info(f"   Lower limit: {lower_limit}")
            logger.info(f"   Upper limit: {upper_limit}")
            
            return filter_condition
            
        except Exception as e:
            logger.warning(f"Failed to build date range filter: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def _validate_time_range_config(self) -> List[str]:
        """Validaciones específicas para estrategia de rango de tiempo"""
        errors = []
        
        # Debe tener columna de filtro para rangos de tiempo
        has_filter_column = (hasattr(self.table_config, 'filter_column') and 
                        self.table_config.filter_column and 
                        self.table_config.filter_column.strip())
        
        if not has_filter_column:
            errors.append("Time range strategy requires filter_column to be specified")
        
        # Verificar que tenga al menos UNA configuración de rango válida
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
        
        if not (has_explicit_range or has_dynamic_range):
            errors.append("Time range strategy requires either (start_value AND end_value) OR delay_incremental_ini")
        
        # Si tiene ambos, dar advertencia pero no error
        if has_explicit_range and has_dynamic_range:
            logger.warning("⚠️ Both explicit range and dynamic range configured. Will use explicit range (higher priority)")
        
        return errors
    
    def _build_explicit_time_range_filter(self) -> str:
        """Construye filtro con valores explícitos de start y end"""
        try:
            filter_column = self.table_config.filter_column.strip()
            start_value = str(self.table_config.start_value).strip()
            end_value = str(self.table_config.end_value).strip()
            
            # Determinar si los valores necesitan quotes (para fechas/strings)
            data_type = getattr(self.table_config, 'filter_data_type', '').lower()
            
            if 'date' in data_type or 'time' in data_type or 'char' in data_type:
                # Para fechas y strings, agregar quotes
                filter_condition = f"{filter_column} BETWEEN '{start_value}' AND '{end_value}'"
            else:
                # Para números, sin quotes
                filter_condition = f"{filter_column} BETWEEN {start_value} AND {end_value}"
            
            logger.info(f"Built explicit time range filter: {filter_condition}")
            return filter_condition
            
        except Exception as e:
            logger.warning(f"Failed to build explicit time range filter: {e}")
            return None
    
    def _should_use_chunking(self) -> bool:
        """Determina si debería usar chunking para time range"""
        return (
            hasattr(self.table_config, 'partition_column') and 
            self.table_config.partition_column and 
            self.table_config.partition_column.strip() != '' and
            getattr(self.table_config, 'source_table_type', '') == 't'
        )
    
    def _get_chunking_column(self) -> str:
        """Obtiene la columna para chunking en time range"""
        if hasattr(self.table_config, 'partition_column') and self.table_config.partition_column:
            return self.table_config.partition_column.strip()
        
        # Usar filter_column pero extraer solo el nombre de la columna
        if hasattr(self.table_config, 'filter_column') and self.table_config.filter_column:
            filter_col_raw = self.table_config.filter_column.strip()
            # Si contiene 'between', tomar solo la parte antes de 'between'
            if 'between' in filter_col_raw.lower():
                return filter_col_raw.split('between')[0].strip()
            else:
                return filter_col_raw.split()[0].strip()
        
        if hasattr(self.table_config, 'id_column') and self.table_config.id_column:
            return self.table_config.id_column.strip()
        
        return None