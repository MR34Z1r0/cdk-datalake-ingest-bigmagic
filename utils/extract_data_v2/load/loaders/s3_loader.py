# -*- coding: utf-8 -*-
import boto3
import uuid
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
                      filename: Optional[str] = None, **kwargs) -> str:
        """
        Load DataFrame to S3
        
        Args:
            df: DataFrame to load
            destination_path: S3 path (without s3:// prefix)
            filename: Optional filename, will generate if not provided
            **kwargs: Additional options
            
        Returns:
            Full S3 path of uploaded file
        """
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
            file_data = self.formatter.format_dataframe(df, **kwargs)
            
            # Ensure destination path doesn't start with /
            if destination_path.startswith('/'):
                destination_path = destination_path[1:]
            
            # Ensure destination path ends with /
            if destination_path and not destination_path.endswith('/'):
                destination_path += '/'
            
            # Build full key
            s3_key = destination_path + filename
            
            # Upload to S3
            full_s3_path = self.s3_helper.put_object(
                object_key=s3_key,
                body=file_data,
                extra_args={'ContentType': self.formatter.get_content_type()}
            )
            
            return full_s3_path
            
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