# -*- coding: utf-8 -*-
import boto3
import uuid
import time
from decimal import Decimal
from datetime import datetime 
from models.file_metadata import FileMetadata
from typing import List, Optional, Dict, Any
import pandas as pd
from interfaces.loader_interface import LoaderInterface
from exceptions.custom_exceptions import LoadError
from aje_libs.common.helpers.s3_helper import S3Helper

class S3Loader(LoaderInterface):
    """S3 implementation of LoaderInterface"""
    
    def __init__(self, bucket_name: str, **kwargs):
        self.bucket_name = bucket_name
        self.s3_helper = S3Helper(bucket_name)
        self.region = kwargs.get('region', 'us-east-1')
        self.formatter = None
    
    def set_formatter(self, formatter):
        """Set the file formatter to use"""
        self.formatter = formatter
    
    def load_dataframe(self, df: pd.DataFrame, destination_path: str, 
                      filename: Optional[str] = None, **kwargs) -> tuple:
        """
        Load DataFrame to S3 with metadata capture
        
        Returns:
            Tuple[str, FileMetadata]: (file_path, file_metadata)
        """
        upload_start = time.time()
        
        try:
            if not self.formatter:
                raise LoadError("No formatter set for S3Loader")
            
            # Generate filename if not provided
            if not filename:
                thread_id = kwargs.get('thread_id', 0)
                chunk_id = kwargs.get('chunk_id', 0)
                unique_id = uuid.uuid4().hex[:8]
                base_name = f"data_thread_{thread_id}_chunk_{chunk_id}_{unique_id}"
                filename = base_name + self.formatter.get_file_extension()
            elif not filename.endswith(self.formatter.get_file_extension()):
                filename += self.formatter.get_file_extension()
            
            # Format DataFrame
            extraction_start = time.time()
            file_data = self.formatter.format_dataframe(df, **kwargs)
            extraction_duration = time.time() - extraction_start
            
            # Ensure destination path
            if destination_path.startswith('/'):
                destination_path = destination_path[1:]
            if destination_path and not destination_path.endswith('/'):
                destination_path += '/'
            
            # Build full key
            s3_key = destination_path + filename
            
            # Upload to S3
            upload_file_start = time.time()
            full_s3_path = self.s3_helper.put_object(
                object_key=s3_key,
                body=file_data,
                extra_args={'ContentType': self.formatter.get_content_type()}
            )
            upload_file_duration = time.time() - upload_file_start
            
            # 🆕 Crear metadata del archivo
            file_metadata = FileMetadata(
                file_path=full_s3_path,
                file_name=filename,
                file_size_bytes=len(file_data),
                file_size_mb=FileMetadata.calculate_file_size_mb(len(file_data)),
                records_count=len(df),
                thread_id=str(kwargs.get('thread_id', 0)),
                chunk_id=kwargs.get('chunk_id', 0),
                partition_index=kwargs.get('partition_index'),
                created_at=datetime.now().isoformat(),
                compression=getattr(self.formatter, 'compression', 'none'),
                format=self.formatter.get_file_extension().replace('.', ''),
                extraction_duration_seconds=Decimal(str(round(extraction_duration, 3))),
                upload_duration_seconds=Decimal(str(round(upload_file_duration, 3))),
                columns_count=len(df.columns) if not df.empty else 0,
                estimated_memory_mb=FileMetadata.estimate_memory_mb(df.shape)
            )
            
            return file_metadata.to_dict()
            
        except Exception as e:
            raise LoadError(f"Failed to load DataFrame to S3: {e}")
    
    def delete_existing(self, path: str) -> bool:
        """Delete existing data at S3 path"""
        try:
            # Remove s3:// prefix if present
            if path.startswith('s3://'):
                path = path[5:]
            
            # Remove bucket name if present in path
            if path.startswith(f"{self.bucket_name}/"):
                path = path[len(self.bucket_name) + 1:]
            
            # List objects with prefix
            objects = self.s3_helper.list_objects(prefix=path)
            
            if objects:
                keys_to_delete = [obj['Key'] for obj in objects]
                result = self.s3_helper.delete_objects(keys_to_delete)
                return True
            
            return True  # No objects to delete is considered success
            
        except Exception as e:
            raise LoadError(f"Failed to delete existing data at {path}: {e}")
    
    def list_files(self, path: str) -> List[str]:
        """List files at S3 path"""
        try:
            # Remove s3:// prefix if present
            if path.startswith('s3://'):
                path = path[5:]
            
            # Remove bucket name if present in path
            if path.startswith(f"{self.bucket_name}/"):
                path = path[len(self.bucket_name) + 1:]
            
            objects = self.s3_helper.list_objects(prefix=path)
            return [f"s3://{self.bucket_name}/{obj['Key']}" for obj in objects]
            
        except Exception as e:
            raise LoadError(f"Failed to list files at {path}: {e}")
    
    def path_exists(self, path: str) -> bool:
        """Check if S3 path exists"""
        try:
            # Remove s3:// prefix if present
            if path.startswith('s3://'):
                path = path[5:]
            
            # Remove bucket name if present in path
            if path.startswith(f"{self.bucket_name}/"):
                path = path[len(self.bucket_name) + 1:]
            
            # Check if it's a specific object or prefix
            if path.endswith('/'):
                # It's a prefix, check if any objects exist with this prefix
                objects = self.s3_helper.list_objects(prefix=path, max_keys=1)
                return len(objects) > 0
            else:
                # It's a specific object
                return self.s3_helper.object_exists(path)
                
        except Exception:
            return False