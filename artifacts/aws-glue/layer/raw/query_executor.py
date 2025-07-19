# query_executor.py
import pandas as pd
import boto3
import gzip
import io
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from aje_libs.common.logger import custom_logger
from aje_libs.common.helpers.secrets_helper import SecretsHelper
from aje_libs.bd.helpers.datafactory_helper import DatabaseFactoryHelper

class QueryExecutor:
    def __init__(self, config):
        self.config = config
        self.logger = custom_logger(__name__)
        self.db_helper = None
        self.s3_client = boto3.client('s3')
    
    def setup_database_connection(self, endpoint_data):
        """Inicializa la conexión a la base de datos"""
        try:
            secrets_helper = SecretsHelper(
                f"{self.config['ENVIRONMENT']}/{self.config['PROJECT_NAME']}/{endpoint_data['TEAM']}/{endpoint_data['DATA_SOURCE']}"
            )
            password = secrets_helper.get_secret_value(endpoint_data["SRC_DB_SECRET"])
            
            db_type = endpoint_data['BD_TYPE']
            additional_params = {}
            
            if db_type == 'oracle':
                additional_params['service_name'] = endpoint_data['SRC_DB_NAME']
            elif db_type == 'mysql':
                additional_params['charset'] = 'utf8mb4'
            
            self.db_helper = DatabaseFactoryHelper.create_helper(
                db_type=db_type,
                server=endpoint_data['SRC_SERVER_NAME'],
                database=endpoint_data['SRC_DB_NAME'],
                username=endpoint_data['SRC_DB_USERNAME'],
                password=password,
                port=int(endpoint_data['DB_PORT_NUMBER']) if endpoint_data['DB_PORT_NUMBER'] else None,
                **additional_params
            )
            
            self.logger.info(f"Database connection initialized for {db_type}")
            
        except Exception as e:
            self.logger.error(f"Error initializing database connection: {str(e)}")
            raise
    
    def _generate_s3_path(self, table_name, strategy_type, thread_id=None, partition_info=None, table_data=None, endpoint_data=None):
        """
        Genera la ruta S3 para el archivo con la estructura:
        TEAM/DATA_SOURCE/ENDPOINT_NAME/SOURCE_TABLE/YEAR/MONTH/DAY/file.csv.gz
        
        Args:
            table_name: nombre de la tabla
            strategy_type: tipo de estrategia utilizada
            thread_id: ID del hilo (para archivos paralelos)
            partition_info: información de partición adicional
            table_data: datos de configuración de la tabla
            endpoint_data: datos del endpoint
        
        Returns:
            tuple: (bucket, key)
        """
        try:
            s3_prefix = self.config.get('S3_RAW_PREFIX', '').rstrip('/')
            
            if not s3_prefix:
                raise ValueError("S3_RAW_PREFIX not configured")
            
            # Extraer bucket del prefijo
            if s3_prefix.startswith('s3://'):
                s3_prefix = s3_prefix[5:]
            
            bucket = s3_prefix.split('/')[0]
            base_path = '/'.join(s3_prefix.split('/')[1:]) if len(s3_prefix.split('/')) > 1 else ''
            
            # Obtener valores de configuración
            team = self.config.get('TEAM', '')
            data_source = self.config.get('DATA_SOURCE', '')
            endpoint_name = self.config.get('ENDPOINT_NAME', '')
            
            # Obtener SOURCE_TABLE de table_data si está disponible
            source_table = table_name  # fallback
            if table_data and 'SOURCE_TABLE' in table_data:
                source_table = table_data['SOURCE_TABLE']
            
            # Generar timestamp y fecha
            now = datetime.now()
            timestamp = now.strftime('%Y%m%d_%H%M%S')
            year = now.strftime('%Y')
            month = now.strftime('%m')
            day = now.strftime('%d')
            
            # Crear nombre de archivo
            if thread_id is not None:
                filename = f"{source_table}_{strategy_type}_part{thread_id:03d}_{timestamp}.csv.gz"
            else:
                filename = f"{source_table}_{strategy_type}_{timestamp}.csv.gz"
            
            # Construir estructura de carpetas: TEAM/DATA_SOURCE/ENDPOINT_NAME/SOURCE_TABLE/YEAR/MONTH/DAY/
            folder_structure = f"{team}/{data_source}/{endpoint_name}/{source_table}/{year}/{month}/{day}"
            
            # Construir key completa
            if base_path:
                s3_key = f"{base_path}/{folder_structure}/{filename}"
            else:
                s3_key = f"{folder_structure}/{filename}"
            
            self.logger.info(f"Generated S3 path: s3://{bucket}/{s3_key}")
            return bucket, s3_key
            
        except Exception as e:
            self.logger.error(f"Error generating S3 path: {str(e)}")
            raise
     
    def execute_single_query(self, query_str):
        """
        Ejecuta un query SQL y retorna DataFrame
        
        Args:
            query_str: String del query SQL a ejecutar
        
        Returns:
            pandas.DataFrame con los resultados
        """
        try:
            if not self.db_helper:
                raise Exception("Database connection not initialized")
            
            self.logger.info(f"Executing query: {query_str[:200]}...")
            
            # Execute query
            if hasattr(self.db_helper, 'execute_query_as_dataframe'):
                df = self.db_helper.execute_query_as_dataframe(query_str)
            else:
                result = self.db_helper.execute_query_as_dict(query_str)
                df = pd.DataFrame(result)
            
            self.logger.info(f"Query executed successfully. Rows returned: {len(df)}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error executing query: {str(e)}")
            raise
    
    def _upload_dataframe_to_s3(self, df, table_name, strategy_type, thread_id=None, partition_info=None, table_data=None, endpoint_data=None):
        """
        Convierte DataFrame a CSV.gz y lo sube a S3
        
        Args:
            df: pandas DataFrame
            table_name: nombre de la tabla
            strategy_type: tipo de estrategia utilizada
            thread_id: ID del hilo
            partition_info: información adicional de partición
            table_data: datos de configuración de la tabla
            endpoint_data: datos del endpoint
        
        Returns:
            dict con información del upload
        """
        try:
            if df.empty:
                self.logger.warning(f"DataFrame is empty for table {table_name}")
                return {
                    'success': False,
                    'error': 'Empty DataFrame',
                    'rows_count': 0
                }
            
            # Generar path S3 con la nueva estructura
            bucket, s3_key = self._generate_s3_path(
                table_name, 
                strategy_type, 
                thread_id, 
                partition_info,
                table_data,
                endpoint_data
            )
            
            # Convertir DataFrame a CSV en memoria
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, encoding='utf-8')
            csv_data = csv_buffer.getvalue()
            
            # Comprimir CSV a gzip en memoria
            gz_buffer = io.BytesIO()
            with gzip.GzipFile(fileobj=gz_buffer, mode='wb') as gz_file:
                gz_file.write(csv_data.encode('utf-8'))
            
            gz_buffer.seek(0)
            
            # Subir a S3
            thread_info = f" (Thread {thread_id})" if thread_id is not None else ""
            self.logger.info(f"Uploading {table_name}{thread_info} to S3: s3://{bucket}/{s3_key}")
            
            self.s3_client.upload_fileobj(
                gz_buffer,
                bucket,
                s3_key,
                ExtraArgs={
                    'ContentType': 'application/gzip',
                    'ContentEncoding': 'gzip'
                }
            )
            
            # Obtener información del archivo
            response = self.s3_client.head_object(Bucket=bucket, Key=s3_key)
            file_size = response['ContentLength']
            
            self.logger.info(f"Successfully uploaded {table_name}{thread_info} to S3. Size: {file_size} bytes, Rows: {len(df)}")
            
            return {
                'success': True,
                's3_uri': f"s3://{bucket}/{s3_key}",
                's3_bucket': bucket,
                's3_key': s3_key,
                'file_size_bytes': file_size,
                'rows_count': len(df),
                'upload_timestamp': datetime.now().isoformat(),
                'thread_id': thread_id
            }
            
        except Exception as e:
            thread_info = f" (Thread {thread_id})" if thread_id is not None else ""
            self.logger.error(f"Error uploading {table_name}{thread_info} to S3: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'rows_count': len(df) if df is not None else 0,
                'thread_id': thread_id
            }

    def execute_single_query_strategy(self, query_info, generation_result, upload_to_s3=True):
        """Ejecuta un solo query y opcionalmente sube a S3"""
        try:
            self.logger.info("Executing single query strategy")
            
            df = self.execute_single_query(query_info['query'])
            
            result = {
                'success': True,
                'data': df if not upload_to_s3 else None,  # No retornar data si se sube a S3
                'total_rows': len(df),
                'queries_executed': 1,
                'strategy_type': query_info['type'],
                'table_name': generation_result['table_name'],
                'execution_metadata': {
                    'query_type': query_info['type'],
                    'thread_id': query_info.get('thread_id', 0)
                }
            }
            
            # Upload to S3 if requested
            if upload_to_s3:
                upload_result = self._upload_dataframe_to_s3(
                    df, 
                    generation_result['table_name'], 
                    query_info['type'],
                    table_data=generation_result['table_data'],
                    endpoint_data=generation_result['endpoint_data']
                )
                result['s3_upload'] = upload_result
                
                if upload_result['success']:
                    result['s3_uri'] = upload_result['s3_uri']
                    result['file_size_bytes'] = upload_result['file_size_bytes']
                    self.logger.info(f"Single query strategy completed with S3 upload: {upload_result['s3_uri']}")
                else:
                    self.logger.error(f"S3 upload failed: {upload_result['error']}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error executing single query strategy: {str(e)}")
            raise

    def execute_multiple_queries_strategy(self, queries, query_config, max_workers=None, upload_to_s3=True):
        """Ejecuta múltiples queries en paralelo y sube cada resultado a S3 individualmente"""
        try:
            if max_workers is None:
                max_workers = min(len(queries), 10)  # Límite por defecto
            
            self.logger.info(f"Executing {len(queries)} queries in parallel with {max_workers} workers")
            
            # Ejecutar queries en paralelo con upload directo a S3
            results = self.execute_queries_parallel_with_s3_upload(
                queries, 
                max_workers, 
                query_config['table_name'],
                query_config['strategy_result']['type'],
                upload_to_s3,
                query_config['table_data'],
                query_config['endpoint_data']
            )
            
            # Procesar resultados
            successful_uploads = []
            failed_queries = []
            total_rows = 0
            total_file_size = 0
            
            for result in results:
                if result['success']:
                    total_rows += result['rows']
                    if result.get('s3_upload_success'):
                        successful_uploads.append(result['s3_upload_info'])
                        total_file_size += result['s3_upload_info']['file_size_bytes']
                else:
                    failed_queries.append({
                        'query_index': result['query_index'],
                        'thread_id': result['thread_id'],
                        'error': result['error']
                    })
            
            if not successful_uploads and upload_to_s3:
                raise Exception("All queries failed or no successful S3 uploads")
            
            return {
                'success': True,
                'data': None,  # No retornamos data combinada cuando usamos S3
                'total_rows': total_rows,
                'queries_executed': len([r for r in results if r['success']]),
                'queries_failed': len(failed_queries),
                'strategy_type': query_config['strategy_result']['type'],
                'table_name': query_config['table_name'],
                's3_files': successful_uploads if upload_to_s3 else [],
                'total_file_size_bytes': total_file_size,
                'execution_metadata': {
                    'total_queries': len(queries),
                    'successful_queries': len([r for r in results if r['success']]),
                    'successful_uploads': len(successful_uploads),
                    'failed_queries': failed_queries if failed_queries else None,
                    'max_workers': max_workers,
                    'upload_strategy': 'parallel_individual_files'
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error in multiple queries strategy: {str(e)}")
            raise

    def execute_queries_parallel_with_s3_upload(self, queries_list, max_workers, table_name, strategy_type, upload_to_s3, table_data=None, endpoint_data=None):
        """
        Ejecuta múltiples queries en paralelo y sube cada resultado directamente a S3
        
        Args:
            queries_list: Lista de queries o lista de dicts con query info
            max_workers: Número máximo de threads
            table_name: Nombre de la tabla
            strategy_type: Tipo de estrategia
            upload_to_s3: Si debe subir a S3
            table_data: Datos de configuración de la tabla
            endpoint_data: Datos del endpoint
        
        Returns:
            List de resultados
        """
        results = []
        
        def execute_query_worker_with_s3(query_info, index):
            """Worker function para ejecutar un query y subirlo a S3"""
            try:
                # Extract query string
                if isinstance(query_info, dict):
                    query_str = query_info['query']
                    thread_id = query_info.get('thread_id', index)
                    query_type = query_info.get('type', 'unknown')
                    partition_info = {
                        'start_range': query_info.get('start_range'),
                        'end_range': query_info.get('end_range'),
                        'partition_index': query_info.get('partition_index')
                    }
                else:
                    query_str = query_info
                    thread_id = index
                    query_type = 'direct'
                    partition_info = None
                
                self.logger.info(f"Worker {thread_id}: Starting query execution")
                
                # Ejecutar query
                df = self.execute_single_query(query_str)
                
                result = {
                    'success': True,
                    'query_index': index,
                    'thread_id': thread_id,
                    'query_type': query_type,
                    'rows': len(df),
                    's3_upload_success': False,
                    's3_upload_info': None
                }
                
                # Subir a S3 si está habilitado
                if upload_to_s3 and not df.empty:
                    upload_result = self._upload_dataframe_to_s3(
                        df, 
                        table_name, 
                        strategy_type, 
                        thread_id, 
                        partition_info,
                        table_data,
                        endpoint_data
                    )
                    
                    result['s3_upload_success'] = upload_result['success']
                    result['s3_upload_info'] = upload_result
                    
                    if not upload_result['success']:
                        self.logger.error(f"Worker {thread_id}: S3 upload failed: {upload_result['error']}")
                
                return result
                
            except Exception as e:
                self.logger.error(f"Worker {thread_id}: Error executing query: {str(e)}")
                return {
                    'success': False,
                    'query_index': index,
                    'thread_id': thread_id,
                    'query_type': query_type if 'query_type' in locals() else 'unknown',
                    'error': str(e),
                    'rows': 0,
                    's3_upload_success': False,
                    's3_upload_info': None
                }
        
        # Ejecutar en paralelo
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Enviar todas las tareas
            future_to_index = {
                executor.submit(execute_query_worker_with_s3, query_info, i): i 
                for i, query_info in enumerate(queries_list)
            }
            
            # Recopilar resultados
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result['success']:
                        if result['s3_upload_success']:
                            self.logger.info(f"Query {index + 1} completed successfully - {result['rows']} rows - Uploaded to S3: {result['s3_upload_info']['s3_uri']}")
                        else:
                            self.logger.info(f"Query {index + 1} completed successfully - {result['rows']} rows - S3 upload: {'disabled' if not upload_to_s3 else 'failed'}")
                    else:
                        self.logger.error(f"Query {index + 1} failed: {result['error']}")
                        
                except Exception as e:
                    self.logger.error(f"Unexpected error in query {index + 1}: {str(e)}")
                    results.append({
                        'success': False,
                        'query_index': index,
                        'thread_id': index,
                        'error': f"Unexpected error: {str(e)}",
                        'rows': 0,
                        's3_upload_success': False,
                        's3_upload_info': None
                    })
        
        # Ordenar resultados por índice
        results.sort(key=lambda x: x['query_index'])
        return results    