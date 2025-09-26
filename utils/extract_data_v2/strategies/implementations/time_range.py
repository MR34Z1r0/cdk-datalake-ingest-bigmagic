# strategies/implementations/time_range.py
from typing import List
from ..base.extraction_strategy import ExtractionStrategy
from ..base.extraction_params import ExtractionParams
from ..base.strategy_types import ExtractionStrategyType
from aje_libs.common.logger import custom_logger

logger = custom_logger(__name__)

class TimeRangeStrategy(ExtractionStrategy):
    """Estrategia para carga por rango de fechas específico"""
    
    def get_strategy_type(self) -> ExtractionStrategyType:
        return ExtractionStrategyType.TIME_RANGE
    
    def build_extraction_params(self) -> ExtractionParams:
        """Construye parámetros para carga por rango de tiempo"""
        logger.info(f"=== TIME RANGE STRATEGY - Building Params ===")
        logger.info(f"Table: {self.extraction_config.table_name}")
        
        # 🎯 TIME RANGE NO USA WATERMARKS - usa rangos específicos
        if self.watermark_storage:
            logger.info("⚠️  Watermark storage provided but not used in time range strategy")
        
        # Crear parámetros básicos
        params = ExtractionParams(
            table_name=self._get_source_table_name(),
            columns=self._parse_columns(),
            metadata=self._build_basic_metadata()
        )
        
        # Agregar filtros de rango de tiempo (SIN watermarks)
        time_range_filters = self._build_time_range_filters()
        for filter_condition in time_range_filters:
            params.add_where_condition(filter_condition)
        
        # Configurar chunking si es apropiado
        if self._should_use_chunking():
            params.chunk_size = self.extraction_config.chunk_size
            params.chunk_column = self._get_chunking_column()
            logger.info(f"Chunking enabled - Size: {params.chunk_size}, Column: {params.chunk_column}")
        
        logger.info(f"Time range extraction params built - Columns: {len(params.columns)}, Where conditions: {len(params.where_conditions)}")
        logger.info("=== END TIME RANGE STRATEGY ===")
        
        return params
    
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
            logger.error("❌ VALIDATION FAILED:")
            for error in validation_errors:
                logger.error(f"  - {error}")
            return False
        
        logger.info("✅ ALL VALIDATION CHECKS PASSED")
        logger.info("=== END VALIDATION ===")
        return True
    
    def estimate_resources(self) -> dict:
        """Estima recursos para carga por rango de tiempo"""
        base_estimate = super().estimate_resources()
        
        # Time range puede variar en tamaño dependiendo del rango
        base_estimate.update({
            'estimated_memory_mb': 750,
            'supports_chunking': self._should_use_chunking(),
            'parallel_safe': True,
            'strategy_complexity': 'medium'
        })
        
        return base_estimate
    
    def _build_time_range_filters(self) -> List[str]:
        """Construye filtros específicos para rango de tiempo con detección automática"""
        filters = []
        
        # Filtro básico de la configuración
        if hasattr(self.table_config, 'filter_exp') and self.table_config.filter_exp:
            clean_filter = self.table_config.filter_exp.replace('"', '').strip()
            if clean_filter:
                filters.append(clean_filter)
        
        # DETECCIÓN AUTOMÁTICA DEL TIPO DE RANGO
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
    
    def _build_date_range_filter(self) -> str:
        """Construye filtro de rango de fechas usando delay_incremental_ini"""
        try:
            from utils.date_utils import get_date_limits
            
            # Limpiar delay value
            clean_delay = self.table_config.delay_incremental_ini.strip().replace("'", "")
            
            # Obtener límites de fecha
            lower_limit, upper_limit = get_date_limits(
                clean_delay,
                getattr(self.table_config, 'filter_data_type', '') or ""
            )
            
            # Construir condición de filtro de rango
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
        
        if hasattr(self.table_config, 'id_column') and self.table_config.id_column:
            return self.table_config.id_column.strip()
        
        return None