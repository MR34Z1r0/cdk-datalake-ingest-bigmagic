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
        
        # 🎯 FULL LOAD NO USA WATERMARKS
        if self.watermark_storage:
            logger.info("⚠️  Watermark storage provided but not used in full load strategy")
        
        # Crear parámetros básicos
        params = ExtractionParams(
            table_name=self._get_source_table_name(),
            columns=self._parse_columns(),
            metadata=self._build_basic_metadata()
        )
        
        # Agregar filtros básicos si existen (SIN watermarks)
        basic_filters = self._build_basic_filters()
        for filter_condition in basic_filters:
            params.add_where_condition(filter_condition)
        
        # Configurar chunking si es apropiado
        if self._should_use_chunking():
            params.chunk_size = self.extraction_config.chunk_size
            params.chunk_column = self._get_chunking_column()
            logger.info(f"Chunking enabled - Size: {params.chunk_size}, Column: {params.chunk_column}")
        
        logger.info(f"Full load extraction params built - Columns: {len(params.columns)}, Where conditions: {len(params.where_conditions)}")
        logger.info("=== END FULL LOAD STRATEGY ===")
        
        return params
    
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
    
    def _parse_columns(self) -> List[str]:
        """Parse column string into list, incluyendo ID_COLUMN"""
        columns_list = []
        
        # Agregar ID_COLUMN si existe
        if hasattr(self.table_config, 'id_column') and self.table_config.id_column and self.table_config.id_column.strip():
            id_column_expr = f"{self.table_config.id_column.strip()} as id"
            columns_list.append(id_column_expr)
        
        # Agregar las columnas regulares
        if self.table_config.columns and self.table_config.columns.strip():
            columns_list.append(self.table_config.columns.strip())
        else:
            columns_list.append('*')
        
        return [', '.join(columns_list)]

    def _get_source_table_name(self) -> str:
        """Obtiene el nombre de la tabla fuente CON JOIN_EXPR si existe"""
        source_table = f"{self.table_config.source_schema}.{self.table_config.source_table}"
        
        # Agregar JOIN_EXPR si existe
        if hasattr(self.table_config, 'join_expr') and self.table_config.join_expr and self.table_config.join_expr.strip():
            source_table += f" {self.table_config.join_expr.strip()}"
        
        return source_table

    def _build_basic_filters(self) -> List[str]:
        """Construye filtros básicos incluyendo FILTER_EXP"""
        filters = []
        
        # Agregar FILTER_EXP si existe
        if hasattr(self.table_config, 'filter_exp') and self.table_config.filter_exp and self.table_config.filter_exp.strip():
            filter_exp = self.table_config.filter_exp.strip().replace('"', '')
            filters.append(f"({filter_exp})")
        
        # Agregar filtro de fechas si está configurado
        if (hasattr(self.table_config, 'filter_column') and self.table_config.filter_column and
            hasattr(self.table_config, 'partition_column') and self.table_config.partition_column):
            
            # Para full load, usar el rango completo configurado
            date_filter = self.table_config.filter_column.replace('{0}', '733408').replace('{1}', '739524')
            filters.append(f"({date_filter})")
        
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