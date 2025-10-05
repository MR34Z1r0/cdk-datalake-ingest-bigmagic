# strategies/implementations/full_load.py
from typing import List
from ..base.extraction_strategy import ExtractionStrategy
from ..base.extraction_params import ExtractionParams
from ..base.strategy_types import ExtractionStrategyType
from aje_libs.common.logger import custom_logger

logger = custom_logger(__name__)

class FullLoadStrategy(ExtractionStrategy):
    """Estrategia para carga completa - simple y directa"""
    
    def get_strategy_type(self) -> ExtractionStrategyType:
        return ExtractionStrategyType.FULL_LOAD
    
    def build_extraction_params(self) -> ExtractionParams:
        """Construye parámetros para carga completa"""
        logger.info(f"=== FULL LOAD STRATEGY - Building Params ===")
        logger.info(f"Table: {self.extraction_config.table_name}")
        logger.info(f"Force Full Load: {self.extraction_config.force_full_load}")
        
        # 🎯 DETECTAR SI DEBERÍA GUARDAR WATERMARK
        should_track_watermark = self._should_track_watermark()
        
        if should_track_watermark:
            logger.info("✅ Full load will track watermark for future incremental loads")
        else:
            logger.info("ℹ️ Full load without watermark tracking")
        
        # Detectar si necesita particionado
        if self._should_use_partitioned_load():
            logger.info("⚠️ Partitioned full load detected - will be handled by orchestrator")
            return self._build_partitioned_params(should_track_watermark)  # ← Pasar el flag
        
        # 📋 CARGA NO PARTICIONADA
        logger.info("📋 Building non-partitioned full load params")
        
        # Construir metadata base
        metadata = self._build_basic_metadata()
        
        # 🔑 Marcar si debe rastrear watermark
        if should_track_watermark:
            metadata['should_track_watermark'] = True
            metadata['watermark_column'] = self.table_config.partition_column
            logger.info(f"📊 Watermark tracking enabled for column: {self.table_config.partition_column}")
        
        # Crear parámetros básicos
        params = ExtractionParams(
            table_name=self._get_source_table_name(),
            columns=self._parse_columns(),
            metadata=metadata
        )
        
        # Agregar filtros básicos (sin fechas hardcodeadas)
        basic_filters = self._build_basic_filters()
        for filter_condition in basic_filters:
            if filter_condition:
                params.add_where_condition(filter_condition)
                logger.info(f"➕ Added filter: {filter_condition}")
        
        logger.info(f"✅ Full load params built - Columns: {len(params.columns)}, Filters: {len(params.where_conditions)}")
        return params
    
    def _should_track_watermark(self) -> bool:
        """
        Determina si esta carga completa debería rastrear watermark
        
        TRUE cuando:
        - Es FORCE_FULL_LOAD (carga completa forzada de una tabla incremental)
        - Existe partition_column configurado
        - Hay watermark storage disponible
        """
        has_partition_column = (
            hasattr(self.table_config, 'partition_column') and 
            self.table_config.partition_column and 
            self.table_config.partition_column.strip()
        )
        
        has_watermark_storage = self.watermark_storage is not None
        
        is_forced_full = self.extraction_config.force_full_load
        
        should_track = has_partition_column and has_watermark_storage and is_forced_full
        
        logger.info(f"🔍 Watermark tracking decision:")
        logger.info(f"   - Has partition column: {has_partition_column}")
        logger.info(f"   - Has watermark storage: {has_watermark_storage}")
        logger.info(f"   - Is forced full load: {is_forced_full}")
        logger.info(f"   - Should track: {should_track}")
        
        return should_track
    
    def _should_use_partitioned_load(self) -> bool:
        """Detecta si la tabla requiere particionado"""
        return (
            hasattr(self.table_config, 'source_table_type') and 
            self.table_config.source_table_type == 't' and
            hasattr(self.table_config, 'partition_column') and 
            self.table_config.partition_column and 
            self.table_config.partition_column.strip() != ''
        )

    def _build_partitioned_params(self, should_track_watermark: bool = False) -> ExtractionParams:
        """
        Construye parámetros especiales para carga particionada
        
        Args:
            should_track_watermark: Si debe rastrear watermark durante esta carga completa
            
        Returns:
            ExtractionParams configurado para particionado
        """
        logger.info("🔧 Building partitioned params for full load")
        
        # Construir table_name con JOIN para particionado
        table_name_with_joins = f"{self.table_config.source_schema}.{self.table_config.source_table}"
        
        if hasattr(self.table_config, 'join_expr') and self.table_config.join_expr and self.table_config.join_expr.strip():
            table_name_with_joins += f" {self.table_config.join_expr.strip()}"
            logger.info(f"📎 Table with JOIN: {table_name_with_joins}")
        
        # Construir metadata completo
        metadata = {
            **self._build_basic_metadata(),
            'needs_partitioning': True,
            'partition_column': self.table_config.partition_column,
            'source_table_type': self.table_config.source_table_type,
            'chunk_size': self.extraction_config.chunk_size
        }
        
        # 🔑 Agregar watermark tracking si es necesario
        if should_track_watermark:
            metadata['should_track_watermark'] = True
            metadata['watermark_column'] = self.table_config.partition_column
            logger.info(f"📊 Partitioned load will track watermark for column: {self.table_config.partition_column}")
        else:
            logger.info(f"ℹ️ Partitioned load without watermark tracking")
        
        # Crear params
        params = ExtractionParams(
            table_name=table_name_with_joins,
            columns=self._parse_columns(),
            metadata=metadata
        )
        
        # Agregar filtros básicos (FILTER_EXP, sin fechas hardcodeadas)
        basic_filters = self._build_basic_filters()
        for filter_condition in basic_filters:
            if filter_condition:  # Solo agregar si no está vacío
                params.add_where_condition(filter_condition)
                logger.info(f"➕ Added filter: {filter_condition}")
        
        logger.info(f"✅ Partitioned params built successfully")
        return params

    def _should_use_partitioned_load(self) -> bool:
        """Detecta si la tabla requiere particionado"""
        return (
            hasattr(self.table_config, 'source_table_type') and 
            self.table_config.source_table_type == 't' and
            hasattr(self.table_config, 'partition_column') and 
            self.table_config.partition_column and 
            self.table_config.partition_column.strip() != ''
        )

    def validate(self) -> bool:
        """Valida configuración para carga completa"""
        logger.info("=== FULL LOAD STRATEGY VALIDATION ===")
        
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
        
        if validation_errors:
            logger.error("❌ VALIDATION FAILED:")
            for error in validation_errors:
                logger.error(f"  - {error}")
            return False
        
        logger.info("✅ ALL VALIDATION CHECKS PASSED")
        logger.info("=== END VALIDATION ===")
        return True
    
    def estimate_resources(self) -> dict:
        """Estima recursos para carga completa"""
        base_estimate = super().estimate_resources()
        
        # Full loads pueden ser más intensivos
        base_estimate.update({
            'estimated_memory_mb': 1000,
            'supports_chunking': self._should_use_chunking(),
            'parallel_safe': True
        })
        
        return base_estimate
    
    def _build_basic_filters(self) -> List[str]:
        """Construye filtros básicos sin complejidad excesiva"""
        filters = []
        
        # Filtro básico de la configuración
        if hasattr(self.table_config, 'basic_filter') and self.table_config.basic_filter:
            filters.append(self.table_config.basic_filter.strip())
        
        # Filtros de fecha si están configurados (simplificado)
        if (hasattr(self.table_config, 'filter_column') and 
            self.table_config.filter_column and 
            hasattr(self.table_config, 'delay_incremental_ini') and
            self.table_config.delay_incremental_ini):
            
            try:
                date_filter = self._build_simple_date_filter()
                if date_filter:
                    filters.append(date_filter)
            except Exception as e:
                logger.warning(f"Could not build date filter: {e}")
        
        return filters
    
    def _build_simple_date_filter(self) -> str:
        """Construye un filtro de fecha simple"""
        try:
            from utils.date_utils import get_date_limits
            
            # Limpiar delay value
            clean_delay = self.table_config.delay_incremental_ini.strip().replace("'", "")
            
            # Obtener límites de fecha
            lower_limit, upper_limit = get_date_limits(
                clean_delay,
                getattr(self.table_config, 'filter_data_type', '') or ""
            )
            
            # Construir condición de filtro
            filter_condition = self.table_config.filter_column.replace(
                '{0}', lower_limit
            ).replace(
                '{1}', upper_limit
            ).replace('"', '')
            
            return filter_condition
            
        except Exception as e:
            logger.warning(f"Failed to build date filter: {e}")
            return None
    
    def _should_use_chunking(self) -> bool:
        """Determina si debería usar chunking"""
        return (
            hasattr(self.table_config, 'partition_column') and 
            self.table_config.partition_column and 
            self.table_config.partition_column.strip() != '' and
            getattr(self.table_config, 'source_table_type', '') == 't'
        )
    
    def _get_chunking_column(self) -> str:
        """Obtiene la columna para chunking"""
        if hasattr(self.table_config, 'partition_column') and self.table_config.partition_column:
            return self.table_config.partition_column.strip()
        
        if hasattr(self.table_config, 'id_column') and self.table_config.id_column:
            return self.table_config.id_column.strip()
        
        return None