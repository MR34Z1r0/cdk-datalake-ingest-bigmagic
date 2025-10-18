# strategies/implementations/incremental.py
from typing import List
from ..base.extraction_strategy import ExtractionStrategy
from ..base.extraction_params import ExtractionParams
from ..base.strategy_types import ExtractionStrategyType
from aje_libs.common.datalake_logger import DataLakeLogger
from models.load_mode import LoadMode

logger = DataLakeLogger.get_logger(__name__)

class IncrementalStrategy(ExtractionStrategy):
    """Estrategia para carga incremental basada en columnas de control"""
    
    def get_strategy_type(self) -> ExtractionStrategyType:
        return ExtractionStrategyType.INCREMENTAL
    
    def build_extraction_params(self) -> ExtractionParams:
        logger.info(f"=== INCREMENTAL STRATEGY ===")
        logger.info(f"Table: {self.extraction_config.table_name}")
        logger.info(f"Load Mode: {self.extraction_config.load_mode.value}")
        
        load_mode = self.extraction_config.load_mode
        
        # 🔄 RESET mode: Limpiar watermark y hacer carga inicial
        if load_mode == LoadMode.RESET:
            logger.info("🔄 RESET mode - doing full load + saving watermark")
            self._reset_watermark_if_exists()
            return self._build_initial_load_params()
        
        # 🆕 INITIAL mode: Hacer carga inicial (full + watermark)
        elif load_mode == LoadMode.INITIAL:
            logger.info("🆕 INITIAL mode - doing full load + saving watermark")
            return self._build_initial_load_params()
        
        # 📊 NORMAL mode: Carga incremental desde watermark
        else:
            logger.info("📊 NORMAL mode - incremental from watermark")
            return self._build_incremental_params()
    
    def _build_initial_load_params(self) -> ExtractionParams:
        """
        Construye parámetros para carga inicial.
        
        Carga COMPLETA sin filtros de watermark, pero marcada para guardar watermark.
        """
        logger.info("🏗️ Building INITIAL load params (full + watermark tracking)")
        
        params = ExtractionParams(
            table_name=self._get_source_table_name(),
            columns=self._parse_columns(),
            metadata=self._build_basic_metadata()
        )
        
        # Solo filtros básicos (NO watermark)
        basic_filters = self._build_basic_filters()
        for filter_condition in basic_filters:
            if filter_condition:
                params.add_where_condition(filter_condition)
                logger.info(f"➕ Basic filter: {filter_condition}")
        
        # 🔢 AGREGAR CHUNKING si es apropiado (igual que incremental normal)
        if self._should_use_chunking():
            params.chunk_size = self.extraction_config.chunk_size
            params.chunk_column = self._get_chunking_column()
            logger.info(f"🔢 Chunking enabled for INITIAL load - Size: {params.chunk_size}, Column: {params.chunk_column}")
        else:
            logger.info(f"ℹ️ Chunking NOT enabled for INITIAL load")
        
        # 🔑 Marcar que debe guardar watermark
        params.metadata['should_track_watermark'] = True
        params.metadata['watermark_column'] = self.table_config.partition_column
        
        logger.info(f"✅ Initial load params built - will track watermark")
        logger.info(f"   Chunking: {'YES' if params.chunk_size else 'NO'}")
        
        return params
    
    def _build_incremental_params(self) -> ExtractionParams:
        """
        Construye parámetros para carga incremental normal.
        
        Carga INCREMENTAL desde último watermark.
        """
        logger.info("📊 Building INCREMENTAL params (with watermark filters)")
        
        params = ExtractionParams(
            table_name=self._get_source_table_name(),
            columns=self._parse_columns(),
            metadata=self._build_basic_metadata()
        )
        
        # Filtros incrementales CON watermark
        incremental_filters = self._build_incremental_filters_with_watermark()
        for filter_condition in incremental_filters:
            params.add_where_condition(filter_condition)
            logger.info(f"➕ Incremental filter: {filter_condition}")
        
        # Configurar chunking si es apropiado
        if self._should_use_chunking():
            params.chunk_size = self.extraction_config.chunk_size
            params.chunk_column = self._get_chunking_column()
            logger.info(f"🔢 Chunking enabled - Size: {params.chunk_size}, Column: {params.chunk_column}")
        
        # Marcar que debe guardar watermark
        params.metadata['should_track_watermark'] = True
        params.metadata['watermark_column'] = self.table_config.partition_column
        
        logger.info(f"✅ Incremental params built")
        
        return params
    
    def _reset_watermark_if_exists(self):
        """Limpia el watermark existente"""
        if not self.watermark_storage:
            return
        
        if not (hasattr(self.table_config, 'partition_column') and 
                self.table_config.partition_column):
            return
        
        try:
            existing = self.watermark_storage.get_last_extracted_value(
                table_name=self.table_config.stage_table_name,
                column_name=self.table_config.partition_column
            )
            
            if existing:
                logger.info(f"🗑️ Resetting watermark: {existing}")
                if hasattr(self.watermark_storage, 'delete_watermark'):
                    self.watermark_storage.delete_watermark(
                        table_name=self.table_config.stage_table_name,
                        column_name=self.table_config.partition_column
                    )
                    logger.info(f"✅ Watermark deleted")
        except Exception as e:
            logger.warning(f"Failed to reset watermark: {e}")
    
    def validate(self) -> bool:
        """Valida configuración para carga incremental"""
        logger.info("=== INCREMENTAL STRATEGY VALIDATION ===")
        
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
        
        # Validaciones específicas para incremental
        incremental_errors = self._validate_incremental_config()
        validation_errors.extend(incremental_errors)
        
        if validation_errors:
            logger.error("❌ VALIDATION FAILED:")
            for error in validation_errors:
                logger.error(f"  - {error}")
            return False
        
        logger.info("✅ ALL VALIDATION CHECKS PASSED")
        logger.info("=== END VALIDATION ===")
        return True
    
    def estimate_resources(self) -> dict:
        """Estima recursos para carga incremental"""
        base_estimate = super().estimate_resources()
        
        # Incrementales suelen ser más ligeros
        base_estimate.update({
            'estimated_memory_mb': 500,
            'supports_chunking': self._should_use_chunking(),
            'parallel_safe': True,
            'strategy_complexity': 'medium'
        })
        
        return base_estimate
    
    def _validate_incremental_config(self) -> List[str]:
        """Validaciones específicas para estrategia incremental"""
        errors = []
        
        # Debe tener al menos una columna de control incremental
        has_filter_column = (hasattr(self.table_config, 'filter_column') and 
                           self.table_config.filter_column and 
                           self.table_config.filter_column.strip())
        
        has_delay_config = (hasattr(self.table_config, 'delay_incremental_ini') and 
                          self.table_config.delay_incremental_ini and 
                          self.table_config.delay_incremental_ini.strip())
        
        has_partition_column = (hasattr(self.table_config, 'partition_column') and 
                              self.table_config.partition_column and 
                              self.table_config.partition_column.strip())
        
        if not (has_filter_column or has_partition_column):
            errors.append("Incremental strategy requires either filter_column or partition_column")
        
        if has_filter_column and not has_delay_config:
            errors.append("filter_column specified but delay_incremental_ini is missing")
        
        return errors
    
    def _build_date_based_incremental_filter(self) -> str:
        """Construye filtro incremental basado en fechas como fallback"""
        if (hasattr(self.table_config, 'filter_column') and 
            self.table_config.filter_column and 
            hasattr(self.table_config, 'delay_incremental_ini') and
            self.table_config.delay_incremental_ini):
            
            try:
                return self._build_incremental_date_filter()
            except Exception as e:
                logger.warning(f"Failed to build date-based incremental filter: {e}")
                return None
        
        return None
    
    def _build_incremental_date_filter(self) -> str:
        """Construye filtro incremental basado en fechas"""
        try:
            from utils.date_utils import get_date_limits
            
            # Limpiar delay value
            clean_delay = self.table_config.delay_incremental_ini.strip().replace("'", "")
            
            # Obtener límites de fecha para incremental
            lower_limit, upper_limit = get_date_limits(
                clean_delay,
                getattr(self.table_config, 'filter_data_type', '') or ""
            )
            
            # Construir condición de filtro incremental
            filter_condition = self.table_config.filter_column.replace(
                '{0}', lower_limit
            ).replace(
                '{1}', upper_limit
            ).replace('"', '')
            
            logger.info(f"Built incremental date filter: {filter_condition}")
            return filter_condition
            
        except Exception as e:
            logger.warning(f"Failed to build incremental date filter: {e}")
            return None
    
    def _build_incremental_filters_with_watermark(self) -> List[str]:
        """Construye filtros incrementales usando watermark storage si está disponible"""
        filters = []
        
        # Filtro básico
        if hasattr(self.table_config, 'filter_exp') and self.table_config.filter_exp:
            clean_filter = self.table_config.filter_exp.replace('"', '').strip()
            if clean_filter:
                filters.append(clean_filter)
        
        # Usar watermark si está disponible
        if (self.watermark_storage and
            hasattr(self.table_config, 'partition_column') and 
            self.table_config.partition_column):
            
            try:
                last_value = self.watermark_storage.get_last_extracted_value(
                    table_name=self.table_config.stage_table_name,
                    column_name=self.table_config.partition_column
                )
                
                if last_value:
                    logger.info(f"Found watermark for {self.table_config.stage_table_name}.{self.table_config.partition_column}: {last_value}")
                    
                    # Detectar tipo y formatear apropiadamente
                    data_type = self._detect_watermark_data_type(str(last_value))
                    formatted_value = self._format_watermark_value_for_sql(last_value)
                    
                    # Construir filtro según el tipo de dato
                    watermark_filter = self._build_typed_filter(
                        self.table_config.partition_column, 
                        formatted_value, 
                        data_type
                    )
                    
                    filters.append(watermark_filter)
                    
                    logger.info(f"Applied {data_type} watermark filter: {watermark_filter}")
                else:
                    logger.info(f"No watermark found for {self.table_config.stage_table_name}, will do full incremental")
                    
            except Exception as e:
                logger.warning(f"Failed to get watermark, falling back to date-based incremental: {e}")
                fallback_filter = self._build_date_based_incremental_filter()
                if fallback_filter:
                    filters.append(fallback_filter)
        else:
            logger.info("No watermark storage available, using date-based incremental")
            date_filter = self._build_date_based_incremental_filter()
            if date_filter:
                filters.append(date_filter)
        
        return filters

    def _build_basic_filters(self) -> List[str]:
        """Construye filtros básicos sin complejidad excesiva (mismo patrón que FullLoadStrategy)"""
        filters = []
        
        # Filtro básico de la configuración (FILTER_EXP)
        if hasattr(self.table_config, 'filter_exp') and self.table_config.filter_exp:
            clean_filter = self.table_config.filter_exp.replace('"', '').strip()
            if clean_filter:
                filters.append(clean_filter)
                logger.info(f"Added basic filter from filter_exp: {clean_filter}")
        
        # Alternativamente buscar basic_filter
        elif hasattr(self.table_config, 'basic_filter') and self.table_config.basic_filter:
            clean_filter = self.table_config.basic_filter.strip()
            if clean_filter:
                filters.append(clean_filter)
                logger.info(f"Added basic filter: {clean_filter}")
        
        return filters

    def _build_typed_filter(self, column_name: str, formatted_value: str, data_type: str) -> str:
        """Construye el filtro según el tipo de dato"""
        
        if data_type == 'datetime':
            # Para datetime, asegurar que la columna también use DATETIME2
            return f"CAST({column_name} AS DATETIME2(6)) > {formatted_value}"
        
        elif data_type in ['int', 'bigint']:
            # Para números, comparación directa
            return f"{column_name} > {formatted_value}"
        
        else:
            # Para otros tipos, usar comparación de strings
            return f"{column_name} > {formatted_value}"

    def _format_watermark_value_for_sql(self, value: str) -> str:
        """Formatea el valor del watermark según el tipo de dato detectado"""
        try:
            value_str = str(value).strip()
            
            # Detectar tipo de dato
            data_type = self._detect_watermark_data_type(value_str)
            
            if data_type == 'datetime':
                return self._format_datetime_watermark(value_str)
            elif data_type == 'int':
                return self._format_int_watermark(value_str)
            elif data_type == 'bigint':
                return self._format_bigint_watermark(value_str)
            else:
                # Fallback: tratar como string
                return f"'{value_str}'"
                
        except Exception as e:
            logger.warning(f"Error formatting watermark value: {e}")
            return f"'{value}'"

    def _detect_watermark_data_type(self, value: str) -> str:
        """Detecta el tipo de dato del watermark"""
        try:
            # Es DATETIME si contiene caracteres típicos de fecha
            if any(char in value for char in ['-', ':', ' ', '.']):
                return 'datetime'
            
            # Es número - detectar si es INT o BIGINT
            try:
                num_value = int(value)
                
                # BIGINT: números muy grandes (típicamente timestamps o IDs grandes)
                # INT: -2,147,483,648 a 2,147,483,647
                # BIGINT: -9,223,372,036,854,775,808 a 9,223,372,036,854,775,807
                
                if abs(num_value) > 2147483647:  # Fuera del rango de INT
                    return 'bigint'
                else:
                    return 'int'
                    
            except ValueError:
                # No es número entero
                return 'unknown'
                
        except Exception:
            return 'unknown'

    def _format_datetime_watermark(self, value: str) -> str:
        """Formatea watermark tipo DATETIME con microsegundos"""
        try:
            from datetime import datetime
            
            # Parsear manteniendo microsegundos completos
            for fmt in ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
                try:
                    dt = datetime.strptime(value, fmt)
                    # Formatear con microsegundos completos y usar CAST para DATETIME2
                    formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S.%f')
                    return f"CAST('{formatted_date}' AS DATETIME2(6))"
                except ValueError:
                    continue
            
            # Fallback si no se puede parsear
            return f"CAST('{value}' AS DATETIME2(6))"
            
        except Exception as e:
            logger.warning(f"Error formatting datetime watermark: {e}")
            return f"CAST('{value}' AS DATETIME2(6))"

    def _format_int_watermark(self, value: str) -> str:
        """Formatea watermark tipo INT"""
        try:
            # Validar que es un entero válido
            int_value = int(value)
            return str(int_value)
        except ValueError:
            logger.warning(f"Invalid INT value for watermark: {value}")
            return "0"

    def _format_bigint_watermark(self, value: str) -> str:
        """Formatea watermark tipo BIGINT"""
        try:
            # Validar que es un entero válido (puede ser muy grande)
            bigint_value = int(value)
            return str(bigint_value)
        except ValueError:
            logger.warning(f"Invalid BIGINT value for watermark: {value}")
            return "0"

    def _build_incremental_id_filter(self) -> str:
        """Construye filtro incremental basado en ID"""
        try:
            partition_column = self.table_config.partition_column.strip()
            start_value = str(self.table_config.start_value).strip()
            
            # Construir filtro básico de ID mayor que valor inicial
            filter_condition = f"{partition_column} > {start_value}"
            
            # Si hay end_value, agregar límite superior
            if (hasattr(self.table_config, 'end_value') and 
                self.table_config.end_value and 
                str(self.table_config.end_value).strip()):
                
                end_value = str(self.table_config.end_value).strip()
                filter_condition += f" AND {partition_column} <= {end_value}"
            
            logger.info(f"Built incremental ID filter: {filter_condition}")
            return filter_condition
            
        except Exception as e:
            logger.warning(f"Failed to build incremental ID filter: {e}")
            return None
    
    def _should_use_chunking(self) -> bool:
        """Determina si debería usar chunking para incremental"""
        return (
            hasattr(self.table_config, 'partition_column') and 
            self.table_config.partition_column and 
            self.table_config.partition_column.strip() != '' and
            getattr(self.table_config, 'source_table_type', '') == 't'
        )
    
    def _get_chunking_column(self) -> str:
        """Obtiene la columna para chunking en incremental"""
        if hasattr(self.table_config, 'partition_column') and self.table_config.partition_column:
            return self.table_config.partition_column.strip()
        
        if hasattr(self.table_config, 'id_column') and self.table_config.id_column:
            return self.table_config.id_column.strip()
        
        return None