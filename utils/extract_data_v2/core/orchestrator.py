# -*- coding: utf-8 -*-
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Dict, Any, Optional
import pandas as pd

from models.extraction_config import ExtractionConfig
from models.table_config import TableConfig
from models.database_config import DatabaseConfig
from models.extraction_result import ExtractionResult
from interfaces.extractor_interface import ExtractorInterface
from models.load_mode import LoadMode 
from interfaces.loader_interface import LoaderInterface
from interfaces.monitor_interface import MonitorInterface
from interfaces.strategy_interface import StrategyInterface
from interfaces.watermark_interface import WatermarkStorageInterface
from extract.extractor_factory import ExtractorFactory
from load.loader_factory import LoaderFactory
from load.watermark_factory import WatermarkStorageFactory
from monitoring.monitor_factory import MonitorFactory
from strategies.strategy_factory import StrategyFactory
from utils.csv_loader import CSVConfigLoader
from exceptions.custom_exceptions import *
from config.settings import settings
            

class DataExtractionOrchestrator:
    """Main orchestrator for the data extraction process"""
    
    def __init__(self, extraction_config: ExtractionConfig, monitor: MonitorInterface = None, process_guid: str = None):
        self.extraction_config = extraction_config
        self.monitor = monitor  # Recibir monitor desde main
        self.process_guid = process_guid
        self.table_config: Optional[TableConfig] = None
        self.database_config: Optional[DatabaseConfig] = None
    
        # Inicializar logger - AGREGAR ESTA LÍNEA
        from aje_libs.common.datalake_logger import DataLakeLogger
        self.logger = DataLakeLogger.get_logger(__name__)
        
        if self.process_guid:
            self.logger.info(f"🆔 Orchestrator initialized with process_guid: {self.process_guid}")
        
        # Components
        self.extractor: Optional[ExtractorInterface] = None
        self.loader: Optional[LoaderInterface] = None
        self.monitor: Optional[MonitorInterface] = None
        self.strategy: Optional[StrategyInterface] = None
        self.watermark_storage: Optional[WatermarkStorageInterface] = None
        
        # Results tracking
        self.extraction_result: Optional[ExtractionResult] = None
        
    def execute(self) -> ExtractionResult:
        """Execute the complete data extraction process"""
        start_time = datetime.now()
        
        try:
            # Initialize all components
            self._initialize_components()
            
            self.logger.info(
                f"🚀 Starting extraction - "
                f"table: {self.extraction_config.table_name} "
                f"process_guid: {self.process_guid}")
            
            # Log start
            self.monitor.log_start(
                self.extraction_config.table_name,
                self.strategy.get_strategy_name(),
                self._build_metadata()
            )
            
            # Validate configuration
            self._validate_configuration()
            
            # Execute extraction strategy
            extraction_result = self._execute_extraction_strategy()
            
            if extraction_result.records_extracted > 0:
                self.monitor.log_success(extraction_result)
            
            return extraction_result
            
        except Exception as e:
            error_message = f"Extraction failed: {str(e)}"
            
            self.logger.error(
                f"❌ {error_message} "
                f"table: {self.extraction_config.table_name} "
                f"process_guid: {self.process_guid}",
                exc_info=True
            )
            
            # Log error
            if self.monitor:
                self.monitor.log_error(
                    self.extraction_config.table_name,
                    error_message,
                    self._build_metadata()
                )
                
                # Send notification
                self.monitor.send_notification(
                    f"Failed table: {self.extraction_config.table_name}\n"
                    f"Step: extraction job\n"
                    f"Error: {error_message}",
                    is_error=True
                )
            
            # Create error result
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            error_result = ExtractionResult(
                success=False,
                table_name=self.extraction_config.table_name,
                records_extracted=0,
                files_created=[],
                execution_time_seconds=execution_time,
                strategy_used=self.strategy.get_strategy_name() if self.strategy else "unknown",
                error_message=error_message,
                start_time=start_time,
                end_time=end_time
            )
            
            raise ExtractionError(error_message) from e
            
        finally:
            # Cleanup resources
            self._cleanup()
    
    def _initialize_components(self):
        """Initialize all components needed for extraction"""
        
        # Load configurations
        self._load_configurations()
        
        # Create extractor
        self.extractor = ExtractorFactory.create(
            db_type=self.database_config.db_type,
            config=self.database_config
        )
        
        # Test database connection
        if not self.extractor.test_connection():
            raise ConnectionError("Failed to connect to database")
        
        # 🎯 SOLO CREAR WATERMARK STORAGE SI ES NECESARIO
        watermark_config = self._build_watermark_storage_config()
        
        # Determinar si la estrategia necesita watermarks
        strategy_needs_watermark = self._strategy_needs_watermark_storage()
        
        if strategy_needs_watermark:
            self.watermark_storage = WatermarkStorageFactory.create(
                storage_type=settings.get('WATERMARK_STORAGE_TYPE', 'dynamodb'),
                **watermark_config
            )
            self.logger.info(f"Watermark storage initialized: {type(self.watermark_storage).__name__}")
        else:
            self.watermark_storage = None
            self.logger.info("Watermark storage not needed for this strategy")

        # Create loader
        loader_config = self._build_loader_config()
        self.loader = LoaderFactory.create(
            loader_type=settings.get('LOADER_TYPE', 's3'),
            output_format=self.extraction_config.output_format,
            **loader_config
        )
        
        if not self.monitor:
            self.monitor = MonitorFactory.create(
                'dynamodb',
                table_name=self.extraction_config.dynamo_logs_table,
                project_name=self.extraction_config.data_source,
                team=self.extraction_config.team,
                data_source=self.extraction_config.data_source,
                endpoint_name=self.extraction_config.endpoint_name,
                environment=self.extraction_config.environment,
                sns_topic_arn=self.extraction_config.topic_arn
            )
        
        self.strategy = StrategyFactory.create(
            table_config=self.table_config,
            extraction_config=self.extraction_config,
            watermark_storage=self.watermark_storage  # 👈 AQUÍ SE PASA
        ) 
    
    def _strategy_needs_watermark_storage(self) -> bool:
        """Determina si la estrategia necesita watermark storage"""
        
        # Si force_full_load está activo, no usar watermarks
        if self.extraction_config.force_full_load:
            return False
        
        # Solo las estrategias incrementales necesitan watermarks
        load_type = self.table_config.load_type.lower().strip() if self.table_config.load_type else 'full'
        
        incremental_types = ['incremental']
        
        if load_type in incremental_types:
            # Verificar que tenga los campos necesarios para incremental con watermarks
            has_partition_column = (
                hasattr(self.table_config, 'partition_column') and 
                self.table_config.partition_column and 
                self.table_config.partition_column.strip()
            )
            
            return has_partition_column
        
        return False

    def _load_configurations(self):
        """Load table and database configurations from CSV files"""
        try:
            # Determine if using S3 or local files
            use_s3 = settings.is_aws_s3
            # Load CSV configurations
            if use_s3:
                tables_data = CSVConfigLoader.load_from_s3(settings.get('TABLES_CSV_S3'))
                credentials_data = CSVConfigLoader.load_from_s3(settings.get('CREDENTIALS_CSV_S3'))
                columns_data = CSVConfigLoader.load_from_s3(settings.get('COLUMNS_CSV_S3'))
            else:
                tables_data = CSVConfigLoader.load_from_local(settings.get('TABLES_CSV_S3'))
                credentials_data = CSVConfigLoader.load_from_local(settings.get('CREDENTIALS_CSV_S3'))
                columns_data = CSVConfigLoader.load_from_local(settings.get('COLUMNS_CSV_S3'))
            # Find table configuration
            table_row = CSVConfigLoader.find_config_by_criteria(
                tables_data,
                STAGE_TABLE_NAME=self.extraction_config.table_name
            )
            
            # Find database configuration
            db_row = CSVConfigLoader.find_config_by_criteria(
                credentials_data,
                ENDPOINT_NAME=self.extraction_config.endpoint_name,
                ENV=self.extraction_config.environment
            )
            
            # Build configurations
            self.table_config = self._build_table_config(table_row)
            self.database_config = self._build_database_config(db_row)
            
        except Exception as e:
            raise ConfigurationError(f"Failed to load configurations: {e}")
    
    def _build_table_config(self, table_row: Dict[str, Any]) -> TableConfig:
        """Build TableConfig from CSV row""" 
        
        self.logger.info("=== BUILDING TABLE CONFIG ===")
        self.logger.info(f"Raw CSV row LOAD_TYPE: '{table_row.get('LOAD_TYPE', '')}'")
        
        # Apply load type logic - default to 'full' if not explicitly set
        load_type = table_row.get('LOAD_TYPE', '').strip()
        self.logger.info(f"After strip - load_type: '{load_type}'")
        
        if not load_type:
            load_type = 'full'
            self.logger.info(f"Empty load_type, setting to 'full': '{load_type}'")
        
        self.logger.info(f"Before force_full_load check - load_type: '{load_type}'")
        self.logger.info(f"force_full_load setting: {self.extraction_config.force_full_load}")
        
        # Apply force full load override
        if self.extraction_config.force_full_load and load_type == 'incremental':
            load_type = 'full'
            self.logger.info(f"Applied force_full_load override: '{load_type}'")
        
        self.logger.info(f"Final load_type: '{load_type}'")
        self.logger.info("=== END BUILDING TABLE CONFIG ===")
        
        return TableConfig(
            stage_table_name=table_row.get('STAGE_TABLE_NAME', ''),
            source_schema=table_row.get('SOURCE_SCHEMA', ''),
            source_table=table_row.get('SOURCE_TABLE', ''),
            columns=self._process_columns_field(table_row.get('COLUMNS', '')),
            load_type=load_type,
            source_table_type=table_row.get('SOURCE_TABLE_TYPE', ''),
            partition_mode=table_row.get('PARTITION_MODE', 'AUTO'),
            id_column=table_row.get('ID_COLUMN'),
            partition_column=table_row.get('PARTITION_COLUMN'),
            filter_exp=table_row.get('FILTER_EXP'),
            filter_column=table_row.get('FILTER_COLUMN'),
            filter_data_type=table_row.get('FILTER_DATA_TYPE'),
            join_expr=table_row.get('JOIN_EXPR'),
            delay_incremental_ini=table_row.get('DELAY_INCREMENTAL_INI'),
            delay_incremental_end=table_row.get('DELAY_INCREMENTAL_END'),  # 👈 NUEVO
            start_value=table_row.get('START_VALUE'),
            end_value=table_row.get('END_VALUE')
        )
    
    def _build_database_config(self, db_row: Dict[str, Any]) -> DatabaseConfig:
        """Build DatabaseConfig from CSV row"""
    
        # Construir el nombre del secreto basado en la configuración de extracción
        secret_name = f"{self.extraction_config.environment.lower()}/{self.extraction_config.project_name}/{self.extraction_config.team}/{self.extraction_config.data_source}"
        
        return DatabaseConfig(
            endpoint_name=db_row.get('ENDPOINT_NAME', ''),
            db_type=db_row.get('BD_TYPE', ''),
            server=db_row.get('SRC_SERVER_NAME', ''),
            database=db_row.get('SRC_DB_NAME', ''),
            username=db_row.get('SRC_DB_USERNAME', ''),
            secret_key=db_row.get('SRC_DB_SECRET', ''),
            port=int(db_row.get('DB_PORT_NUMBER')) if db_row.get('DB_PORT_NUMBER') else None,
            secret_name=secret_name
        )
    
    def _build_watermark_storage_config(self) -> Dict[str, Any]:
        """Build watermark storage configuration"""
        storage_type = settings.get('WATERMARK_STORAGE_TYPE', 'dynamodb')
        
        if storage_type == 'dynamodb':
            return {
                'table_name': settings.get('WATERMARK_TABLE', 'extraction-watermarks'),
                'project_name': self.extraction_config.project_name
            }
        elif storage_type == 'csv':
            return {
                'csv_file_path': settings.get('WATERMARK_CSV_PATH', './data/watermarks.csv'),
                'project_name': self.extraction_config.project_name
            }
        
        return {}
    
    def _process_columns_field(self, columns_str: str) -> str:
        """Process columns field to handle SQL Server identifier issues"""
        if not columns_str or columns_str.strip() == '':
            return columns_str
            
        # Clean problematic double quotes
        clean_columns = columns_str.strip()
        
        # Remove wrapping quotes or all quotes
        double_quote_count = clean_columns.count('"')
        if double_quote_count > 0:
            if clean_columns.startswith('"') and clean_columns.endswith('"') and double_quote_count == 2:
                clean_columns = clean_columns[1:-1]
            else:
                clean_columns = clean_columns.replace('"', '')
        
        return clean_columns
    
    def _build_loader_config(self) -> Dict[str, Any]:
        """Build loader configuration"""
        config = {}
        
        if settings.get('LOADER_TYPE', 's3') == 's3':
            config['bucket_name'] = settings.get('S3_RAW_BUCKET')
            config['region'] = settings.get('REGION')
        
        return config
    
    def _build_monitor_config(self) -> Dict[str, Any]:
        """Build monitor configuration"""
        config = {}
        
        if settings.get('MONITOR_TYPE', 'dynamodb') == 'dynamodb':
            config['table_name'] = settings.get('DYNAMO_LOGS_TABLE')
            config['project_name'] = self.extraction_config.project_name
            config['sns_topic_arn'] = settings.get('TOPIC_ARN')
        
        return config
    
    def _validate_configuration(self):
        """Validate all configurations"""
        
        self.logger.info("=== STARTING CONFIGURATION VALIDATION ===")
        
        try:
            self.logger.info("Validating strategy configuration...")
            if not self.strategy.validate_config():
                self.logger.error("❌ Strategy validation failed")
                raise ConfigurationError("Invalid strategy configuration")
            else:
                self.logger.info("✅ Strategy validation passed")
        except Exception as e:
            self.logger.error(f"Error during strategy validation: {str(e)}")
            self.logger.error(f"Exception type: {type(e).__name__}") 
            raise
        
        self.logger.info("=== CONFIGURATION VALIDATION COMPLETED ===")
    
    def _execute_extraction_strategy(self) -> ExtractionResult:
        """Execute the selected extraction strategy"""
        start_time = datetime.now()
        strategy_name = self.strategy.get_strategy_name()
        
        self.logger.info(
            f"📊 Executing {strategy_name} strategy - "
            f"table: {self.extraction_config.table_name} "
            f"process_guid: {self.process_guid}"
        )
        
        # Get destination path
        destination_path = self._build_destination_path()
        
        # Delete existing files for strategies that require it
        if self.extraction_config.load_mode in [LoadMode.INITIAL, LoadMode.RESET]:
            self.logger.info(f"{strategy_name} strategy - deleting existing files")
            self.loader.delete_existing(destination_path)
        
        # 🔧 FIX: Generate queries based on strategy
        queries = self.strategy.generate_queries()
        
        if not queries:
            raise ExtractionError("No queries generated by strategy")
        
        # Execute queries with controlled concurrency
        files_created, files_metadata, total_records = self._execute_queries_parallel(queries)
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        if total_records == 0:
            self.logger.warning(f"⚠️ No records extracted for table {self.extraction_config.table_name}")
            
            if self.monitor:
                self.monitor.log_warning(
                    self.extraction_config.table_name,
                    "No data extracted - Table is empty or no records match filter criteria",
                    self._build_metadata()
                )
        
        # Create result with enriched metadata
        result = ExtractionResult(
            success=True,
            table_name=self.extraction_config.table_name,
            records_extracted=total_records,
            files_created=files_created,
            execution_time_seconds=execution_time,
            strategy_used=self.strategy.get_strategy_name(),
            metadata=self._build_metadata(),
            start_time=start_time,
            end_time=end_time,
            files_metadata=files_metadata
        )
        
        # Log with aggregated metrics
        if files_metadata:
            total_size = sum(f.get('file_size_mb', 0) for f in files_metadata)
            avg_size = total_size / len(files_metadata) if files_metadata else 0
            
            self.logger.info(
                f"📊 Extraction Summary - "
                f"Files: {len(files_created)}, "
                f"Total Size: {total_size:.2f}MB, "
                f"Avg Size: {avg_size:.2f}MB"
            )
        
        self.extraction_result = result
        return result
    
    def _execute_queries_parallel(self, queries: List[Dict[str, Any]]) -> tuple:
        """Execute queries with controlled parallel processing"""
        max_workers = min(self.extraction_config.max_threads, len(queries))
        files_created = []
        all_files_metadata = []  # 🆕 Agregar lista para metadata
        total_records = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_query = {
                executor.submit(self._execute_single_query, i, query): query
                for i, query in enumerate(queries)
            }
            
            for future in as_completed(future_to_query):
                query = future_to_query[future]
                try:
                    # 🔧 FIX: Ahora recibe 3 valores
                    query_files, query_metadata, record_count = future.result()
                    
                    if query_files:
                        if isinstance(query_files, list):
                            files_created.extend(query_files)
                        else:
                            files_created.append(query_files)
                    
                    # 🆕 Agregar metadata
                    if query_metadata:
                        if isinstance(query_metadata, list):
                            all_files_metadata.extend(query_metadata)
                        else:
                            all_files_metadata.append(query_metadata)
                            
                    total_records += record_count
                except Exception as e:
                    raise ExtractionError(f"Failed to execute query: {e}")
        
        # Handle empty result case
        if total_records == 0:
            empty_files, empty_records = self._handle_empty_result()
            files_created = empty_files
            total_records = empty_records
        
        return files_created, all_files_metadata, total_records
    
    def _execute_single_query(self, thread_id: int, query_metadata: Dict[str, Any]) -> tuple:
        """Execute a single query and load the data"""
        query = query_metadata['query']
        metadata = query_metadata.get('metadata', {})
        
        self.logger.info(
            f"🔍 Thread {thread_id} - Starting query execution - "
            f"process_guid: {self.process_guid}"
        )
        
        if metadata.get('query_type') == 'min_max' and metadata.get('needs_partitioned_queries'):
            return self._handle_min_max_query(query, metadata)  # Ya retorna 3 valores
 
        files_created = []
        files_metadata = []
        total_records = 0
        max_extracted_value = None
        
        try:   
            self.logger.info(
                f"🔍 Thread {thread_id} - Strategy: {self.strategy.get_strategy_name()} - "
                f"process_guid: {self.process_guid}"
            )         
            self.logger.info(f"🔍 DEBUG Partition column: '{self.table_config.partition_column}'")
            self.logger.info(f"🔍 DEBUG Watermark storage available: {self.watermark_storage is not None}")

            # Extract data parameters
            chunk_size = metadata.get('chunk_size', self.extraction_config.chunk_size)
            chunking_params = metadata.get('chunking_params', {})
            order_by = chunking_params.get('order_by')

            self.logger.info(f"🔍 DEBUG Chunk size: {chunk_size}")
            self.logger.info(f"🔍 DEBUG Order by: {order_by}")
            self.logger.info(f"🔍 DEBUG Chunking params: {chunking_params}")

            data_iterator = self.extractor.extract_data(query, chunk_size, order_by)
            destination_path = metadata.get('destination_path', self._build_destination_path())

            chunk_count = 0
            for chunk_df in data_iterator:
                if chunk_df is not None and not chunk_df.empty:
                    self.logger.info(f"🔍 Processing chunk {chunk_count + 1} with {len(chunk_df)} rows")
                    self.logger.info(
                        f"🔍 Thread {thread_id} - Processing chunk {chunk_count + 1} "
                        f"with {len(chunk_df)} rows - "
                        f"process_guid: {self.process_guid}"
                    )
                    
                    # 🎯 ACTUALIZAR MAX VALUE (para watermark)
                    if (self.table_config.partition_column and 
                        self.table_config.partition_column in chunk_df.columns):                        
                        chunk_max = chunk_df[self.table_config.partition_column].max()
                        if max_extracted_value is None or chunk_max > max_extracted_value:
                            max_extracted_value = chunk_max
                            self.logger.info(f"🔍 Updated max value: {max_extracted_value}")
                    
                    # ✅ CARGAR CHUNK UNA SOLA VEZ
                    file_path = self.loader.load_dataframe(
                        chunk_df, 
                        destination_path, 
                        thread_id=f"{thread_id}_{chunk_count}"
                    )
                    
                    if file_path:
                        files_created.append(file_path)
                    
                    total_records += len(chunk_df)
                    chunk_count += 1
            
            # 🎯 CONFIRMAR WATERMARK (si aplica)
            strategy_name = self.strategy.get_strategy_name().lower()
            is_incremental = strategy_name in ['incremental', 'incrementalstrategy']
            should_track = metadata.get('should_track_watermark', False)
            
            self.logger.info(f"🔍 DEBUG is_incremental: {is_incremental}")
            self.logger.info(f"🔍 DEBUG should_track: {should_track}")
            
            if (max_extracted_value is not None and 
                self.watermark_storage and 
                self.table_config.partition_column and
                (is_incremental or should_track)):
                
                # Guardar watermark
                if hasattr(self.watermark_storage, 'confirm'):
                    success = self.watermark_storage.confirm(
                        table_name=self.table_config.stage_table_name,
                        column_name=self.table_config.partition_column,
                        additional_metadata={
                            'confirmed_at': datetime.now().isoformat(),
                            'records_extracted': total_records,
                            'files_created': len(files_created),
                            'thread_id': thread_id,
                            'strategy': self.strategy.get_strategy_name()
                        }
                    )
                    if success:
                        self.logger.info(f"✅ Watermark saved: {max_extracted_value}")
                else:
                    # Método directo si no soporta transacciones
                    self.watermark_storage.update_last_extracted_value(
                        table_name=self.table_config.stage_table_name,
                        column_name=self.table_config.partition_column,
                        value=str(max_extracted_value)
                    )
                    self.logger.info(f"✅ Watermark saved: {max_extracted_value}")
            else:
                self.logger.info("🔍 No watermark to save")
            
            return files_created, files_metadata, total_records
            
        except Exception as e:
            self.logger.error(f"❌ Error in query execution: {e}")
            raise ExtractionError(f"Failed to execute query for thread {thread_id}: {e}")
    
    def _handle_min_max_query(self, min_max_query: str, metadata: Dict[str, Any]) -> tuple:
        """Maneja la ejecución de query MIN/MAX y genera queries particionadas"""
        self.logger.info("🔍 Handling MIN/MAX query for partitioned load")
        
        try:
            # 1. Ejecutar query MIN/MAX
            df_results = []
            for chunk_df in self.extractor.extract_data(min_max_query):
                df_results.append(chunk_df)
            
            if not df_results or df_results[0].empty:
                self.logger.warning(f"⚠️ MIN/MAX query returned no results. Skipping table.")
                # Retornar resultado vacío pero válido
                return [], [],0
            
            # 2. Extraer valores MIN/MAX
            df = df_results[0]
            min_val = df['min_val'].iloc[0]
            max_val = df['max_val'].iloc[0]
            
            if min_val is None or max_val is None:
                self.logger.warning(f"⚠️ No valid data found for partitioning (MIN/MAX returned None). Skipping table.")
                # Retornar resultado vacío pero válido
                return [], [],0

            min_val = int(min_val)
            max_val = int(max_val)
            
            self.logger.info(f"🔍 MIN/MAX results: min={min_val}, max={max_val}")
            
            # 3. Generar queries particionadas
            partitioned_queries = self._generate_partitioned_queries(min_val, max_val, metadata)
            
            # 4. Ejecutar queries particionadas en paralelo
            files_created, files_metadata, total_records = self._execute_partitioned_queries(partitioned_queries)
            return files_created, files_metadata, total_records  # 🔧 Retornar 3 valores
            
        except Exception as e:
            raise ExtractionError(f"Failed to handle MIN/MAX query: {e}")
    
    def _generate_partitioned_queries(self, min_val: int, max_val: int, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Genera queries particionadas basadas en los valores MIN/MAX"""
        partition_column = metadata['partition_column']
        max_threads = min(self.extraction_config.max_threads, 30)
        
        range_size = max_val - min_val
        number_threads = min(max_threads, max(1, range_size))
        increment = max(1, range_size // number_threads)
        
        self.logger.info(f"🔍 Generating {number_threads} partitioned queries - Range: {range_size}, Increment: {increment}")
        
        queries = []
        for i in range(number_threads):
            start_value = int(min_val + (increment * i))
            
            if i == number_threads - 1:
                end_value = max_val + 1  # Incluir el último valor
            else:
                end_value = int(min_val + (increment * (i + 1)))
            
            # Construir query particionada usando la configuración de tabla
            partitioned_query = self._build_partitioned_query(partition_column, start_value, end_value)
            
            queries.append({
                'query': partitioned_query,
                'thread_id': i,
                'metadata': {
                    **metadata,
                    'partition_index': i,
                    'start_range': start_value,
                    'end_range': end_value,
                    'chunking_params': self._get_chunking_params_for_partition()
                }
            })
        
        return queries

    def _parse_columns_for_partition(self) -> str:
        """Construye las columnas para queries particionadas"""
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
        
        return ', '.join(columns_list)

    def _get_chunking_params_for_partition(self) -> dict:
        """Obtiene parámetros de chunking para particiones"""
        chunking_params = {}
        
        # Si tiene partition_column configurado, usar para chunking
        if hasattr(self.table_config, 'partition_column') and self.table_config.partition_column:
            chunking_params['order_by'] = self.table_config.partition_column.strip()
        
        # Agregar chunk_size si está configurado
        if self.extraction_config.chunk_size:
            chunking_params['chunk_size'] = self.extraction_config.chunk_size
        
        return chunking_params

    def _build_partitioned_query(self, partition_column: str, start_value: int, end_value: int) -> str:
        """Construye una query particionada individual"""
        # Construir columnas con ID_COLUMN si existe
        columns = self._parse_columns_for_partition()
        
        # Construir FROM con JOINs
        from_clause = f"{self.table_config.source_schema}.{self.table_config.source_table}"
        if hasattr(self.table_config, 'join_expr') and self.table_config.join_expr:
            from_clause += f" {self.table_config.join_expr}"
        
        # Construir WHERE con partición y filtros
        where_conditions = [f"{partition_column} >= {start_value} AND {partition_column} < {end_value}"]
        
        if hasattr(self.table_config, 'filter_exp') and self.table_config.filter_exp:
            filter_exp = self.table_config.filter_exp.strip().replace('"', '')
            where_conditions.append(f"({filter_exp})")
        
        query = f"SELECT {columns} FROM {from_clause} WHERE {' AND '.join(where_conditions)}"
        
        self.logger.info(f"🔍 Generated partitioned query: Range {start_value}-{end_value}")
        return query

    def _execute_partitioned_queries(self, partitioned_queries: List[Dict[str, Any]]) -> tuple:
        """Ejecuta las queries particionadas en paralelo"""
        self.logger.info(
            f"🔍 Executing {len(partitioned_queries)} partitioned queries in parallel - "
            f"process_guid: {self.process_guid}"
        )
        
        files_created = []
        all_files_metadata = []  # 🆕
        total_records = 0
        
        max_workers = min(self.extraction_config.max_threads, len(partitioned_queries))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_query = {
                executor.submit(self._execute_partition_query, i, query): query
                for i, query in enumerate(partitioned_queries)
            }
            
            for future in as_completed(future_to_query):
                try:
                    # 🆕 Ahora recibe metadata también
                    partition_files, partition_metadata, record_count = future.result()
                    
                    if partition_files:
                        files_created.extend(partition_files)
                        all_files_metadata.extend(partition_metadata)
                        
                    total_records += record_count
                except Exception as e:
                    raise ExtractionError(f"Failed to execute partitioned query: {e}")
        
        return files_created, all_files_metadata, total_records

    def _handle_empty_result(self) -> tuple:
        """Handle case where no data was extracted"""
        files_created = []
        total_records = 0
        
        # Create empty file with schema if configured
        if self.table_config and self.table_config.columns:
            columns = [col.strip() for col in self.table_config.columns.split(',')]
            empty_df = pd.DataFrame(columns=columns)
            
            destination_path = self._build_destination_path()
            file_path = self.loader.load_dataframe(
                empty_df,
                destination_path,
                thread_id=0
            )
            files_created.append(file_path)
        
        return files_created, total_records
    
    def _build_destination_path(self) -> str:
        """Build destination path for files"""
        from utils.date_utils import get_date_parts
        
        year, month, day = get_date_parts()
        
        # Get clean table name
        clean_table_name = self._get_clean_table_name()
        
        return f"{self.extraction_config.team}/{self.extraction_config.data_source}/{self.extraction_config.endpoint_name}/{clean_table_name}/year={year}/month={month}/day={day}/"
    
    def _get_clean_table_name(self) -> str:
        """Extract clean table name from SOURCE_TABLE, removing alias after space"""
        if self.table_config and self.table_config.source_table:
            source_table = self.table_config.source_table
        else:
            source_table = self.extraction_config.table_name
        
        # Split by space and take only the first part (table name)
        clean_name = source_table.split()[0] if source_table and ' ' in source_table else source_table
        return clean_name
    
    def _build_metadata(self) -> Dict[str, Any]:
        """Build metadata for logging"""
        metadata = {
            'process_guid': self.process_guid,
            'project_name': self.extraction_config.project_name,
            'team': self.extraction_config.team,
            'data_source': self.extraction_config.data_source,
            'endpoint_name': self.extraction_config.endpoint_name,
            'environment': self.extraction_config.environment,
            'table_name': self.extraction_config.table_name
        }
        
        if self.database_config:
            metadata.update({
                'server': self.database_config.server,
                'username': self.database_config.username,
                'db_type': self.database_config.db_type
            })
        
        if self.strategy:
            metadata['strategy'] = self.strategy.get_strategy_name()
        
        return metadata
    
    def _cleanup(self):
        """Cleanup resources"""
        if self.extractor:
            try:
                self.extractor.close()
            except Exception:
                pass
     
    def _execute_partition_query(self, thread_id: int, query_metadata: Dict[str, Any]) -> tuple:
        """Ejecuta una query particionada individual con metadata completa"""
        self.logger.info(f"🔍 DEBUG: Starting _execute_partition_query for thread {thread_id}")
        
        query = query_metadata['query']
        metadata = query_metadata.get('metadata', {})
        
        files_created = []
        files_metadata = []
        total_records = 0
        
        try:
            chunk_size = metadata.get('chunk_size', self.extraction_config.chunk_size)
            chunking_params = metadata.get('chunking_params', {})
            order_by = chunking_params.get('order_by')
            
            destination_path = metadata.get('destination_path', self._build_destination_path())
            partition_index = metadata.get('partition_index')
            
            chunk_count = 0
            for chunk_df in self.extractor.extract_data(query, chunk_size, order_by):
                chunk_count += 1
                
                if chunk_df is not None and not chunk_df.empty:
                    self.logger.info(f"🔍 DEBUG: Processing chunk {chunk_count} with {len(chunk_df)} rows")
                    
                    # Cargar chunk
                    file_path = self.loader.load_dataframe(
                        chunk_df, 
                        destination_path, 
                        thread_id=f"{thread_id}_{chunk_count-1}",
                        chunk_id=chunk_count-1
                    )
                    
                    if file_path:
                        files_created.append(file_path)
                        
                        # 🆕 Obtener tamaño del archivo desde S3
                        file_size_bytes = self._get_s3_file_size(file_path)
                        file_size_mb = round(file_size_bytes / (1024 * 1024), 2) if file_size_bytes else 0
                        
                        # Construir metadata completa
                        file_name = file_path.split('/')[-1] if '/' in file_path else file_path
                        file_metadata = {
                            'file_path': file_path,
                            'file_name': file_name,
                            'file_size_bytes': file_size_bytes,
                            'file_size_mb': file_size_mb,
                            'records_count': len(chunk_df),
                            'columns_count': len(chunk_df.columns),
                            'thread_id': str(thread_id),
                            'chunk_id': chunk_count - 1,
                            'partition_index': partition_index,
                            'created_at': datetime.now().isoformat(),
                            'compression': 'snappy',  # Por defecto en parquet
                            'format': 'parquet'
                        }
                        files_metadata.append(file_metadata)
                    
                    total_records += len(chunk_df)
                    
                    self.logger.info(
                        f"📁 File created: {file_name} | "
                        f"Size: {file_size_mb}MB | "
                        f"Records: {len(chunk_df):,} | "
                        f"Columns: {len(chunk_df.columns)}"
                    )
            
            self.logger.info(
                f"✅ Thread {thread_id} completed: {total_records:,} records, "
                f"{len(files_created)} files, "
                f"{sum(f['file_size_mb'] for f in files_metadata):.2f}MB total"
            )
            
            return files_created, files_metadata, total_records
            
        except Exception as e:
            self.logger.error(f"❌ ERROR in _execute_partition_query for thread {thread_id}: {e}")
            import traceback
            self.logger.error(f"🔍 Traceback: {traceback.format_exc()}")
            raise ExtractionError(f"Failed to execute partition query for thread {thread_id}: {e}")

    def _get_s3_file_size(self, s3_path: str) -> int:
        """Obtiene el tamaño de un archivo en S3"""
        try:
            # Remover prefijo s3://
            if s3_path.startswith('s3://'):
                s3_path = s3_path[5:]
            
            # Separar bucket y key
            parts = s3_path.split('/', 1)
            if len(parts) != 2:
                return 0
                
            bucket_name, key = parts
            
            # Obtener metadata del archivo
            import boto3
            s3_client = boto3.client('s3')
            response = s3_client.head_object(Bucket=bucket_name, Key=key)
            
            return response['ContentLength']
            
        except Exception as e:
            self.logger.warning(f"No se pudo obtener tamaño de archivo {s3_path}: {e}")
            return 0