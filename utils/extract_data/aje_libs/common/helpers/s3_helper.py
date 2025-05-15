# src/aje_libs/common/helpers/s3_helper.py

import json
import boto3
from botocore.exceptions import ClientError
from typing import Optional, Dict, List, Any, Union, Tuple
import io
import mimetypes
from pathlib import Path

from ..logger import custom_logger

logger = custom_logger(__name__)

class S3Helper:
    """Custom helper for S3 to simplify file operations."""

    def __init__(
        self,
        bucket_name: str,
        region_name: Optional[str] = None,
    ) -> None:
        """
        Initialize the S3 helper.

        :param bucket_name: Name of the S3 bucket.
        :param region_name: AWS region (optional, defaults to boto3 default).
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client("s3", region_name=region_name)
        self.s3_resource = boto3.resource("s3", region_name=region_name)
        self.bucket = self.s3_resource.Bucket(bucket_name)
        self._validate_bucket()
        logger.info(f"Configured helper for S3 bucket: {bucket_name}")

    def _validate_bucket(self) -> None:
        """Validate that the bucket exists and is accessible"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except ClientError as error:
            logger.error(f"Bucket {self.bucket_name} does not exist or is inaccessible")
            raise error

    def upload_file(
        self, 
        file_path: str, 
        object_key: str,
        extra_args: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Upload a file to S3.

        :param file_path: Local path to the file.
        :param object_key: Key name in S3.
        :param extra_args: Extra arguments to pass to upload_file.
        :return: S3 path of the uploaded file.
        """
        s3_path = f"s3://{self.bucket_name}/{object_key}"
        logger.info(f"Uploading file to S3: {s3_path}")
        
        try:
            # Set content type if not provided
            if extra_args is None:
                extra_args = {}
            
            if 'ContentType' not in extra_args:
                content_type, _ = mimetypes.guess_type(file_path)
                if content_type:
                    extra_args['ContentType'] = content_type
            
            self.s3_client.upload_file(
                file_path,
                self.bucket_name,
                object_key,
                ExtraArgs=extra_args
            )
            logger.info(f"File uploaded successfully: {s3_path}")
            return s3_path
        except ClientError as error:
            logger.error(
                f"Failed to upload file - Bucket: {self.bucket_name} | Key: {object_key} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def upload_fileobj(
        self,
        fileobj,
        object_key: str,
        extra_args: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Upload a file object to S3.

        :param fileobj: File-like object to upload.
        :param object_key: Key name in S3.
        :param extra_args: Extra arguments to pass to upload_fileobj.
        :return: S3 path of the uploaded file.
        """
        s3_path = f"s3://{self.bucket_name}/{object_key}"
        logger.info(f"Uploading file object to S3: {s3_path}")
        
        try:
            self.s3_client.upload_fileobj(
                fileobj,
                self.bucket_name,
                object_key,
                ExtraArgs=extra_args
            )
            logger.info(f"File object uploaded successfully: {s3_path}")
            return s3_path
        except ClientError as error:
            logger.error(
                f"Failed to upload file object - Bucket: {self.bucket_name} | Key: {object_key} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def download_file(
        self,
        object_key: str,
        file_path: str,
        extra_args: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Download a file from S3.

        :param object_key: Key name in S3.
        :param file_path: Local path to save the file.
        :param extra_args: Extra arguments to pass to download_file.
        """
        logger.info(f"Downloading file from S3: s3://{self.bucket_name}/{object_key}")
        
        try:
            self.s3_client.download_file(
                self.bucket_name,
                object_key,
                file_path,
                ExtraArgs=extra_args
            )
            logger.info(f"File downloaded successfully to: {file_path}")
        except ClientError as error:
            logger.error(
                f"Failed to download file - Bucket: {self.bucket_name} | Key: {object_key} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def download_fileobj(
        self,
        object_key: str,
        fileobj,
        extra_args: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Download file content to a file object.

        :param object_key: Key name in S3.
        :param fileobj: File-like object to write to.
        :param extra_args: Extra arguments to pass to download_fileobj.
        """
        logger.info(f"Downloading file object from S3: s3://{self.bucket_name}/{object_key}")
        
        try:
            self.s3_client.download_fileobj(
                self.bucket_name,
                object_key,
                fileobj,
                ExtraArgs=extra_args
            )
            logger.info(f"File object downloaded successfully")
        except ClientError as error:
            logger.error(
                f"Failed to download file object - Bucket: {self.bucket_name} | Key: {object_key} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def get_object(self, object_key: str) -> Dict[str, Any]:
        """
        Get object content and metadata.

        :param object_key: Key name in S3.
        :return: Object content and metadata.
        """
        logger.info(f"Getting object from S3: s3://{self.bucket_name}/{object_key}")
        
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=object_key
            )
            logger.info("Object retrieved successfully")
            return response
        except ClientError as error:
            logger.error(
                f"Failed to get object - Bucket: {self.bucket_name} | Key: {object_key} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def put_object(
        self,
        object_key: str,
        body: Union[str, bytes, io.IOBase],
        extra_args: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Put object content to S3.

        :param object_key: Key name in S3.
        :param body: Content to upload.
        :param extra_args: Extra arguments to pass to put_object.
        :return: S3 path of the uploaded object.
        """
        s3_path = f"s3://{self.bucket_name}/{object_key}"
        logger.info(f"Putting object to S3: {s3_path}")
        
        try:
            put_args = {
                'Bucket': self.bucket_name,
                'Key': object_key,
                'Body': body
            }
            
            if extra_args:
                put_args.update(extra_args)
            
            self.s3_client.put_object(**put_args)
            logger.info(f"Object put successfully: {s3_path}")
            return s3_path
        except ClientError as error:
            logger.error(
                f"Failed to put object - Bucket: {self.bucket_name} | Key: {object_key} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def delete_object(self, object_key: str) -> None:
        """
        Delete an object from S3.

        :param object_key: Key name in S3.
        """
        logger.info(f"Deleting object from S3: s3://{self.bucket_name}/{object_key}")
        
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=object_key
            )
            logger.info("Object deleted successfully")
        except ClientError as error:
            logger.error(
                f"Failed to delete object - Bucket: {self.bucket_name} | Key: {object_key} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def delete_objects(self, object_keys: List[str]) -> Dict[str, Any]:
        """
        Delete multiple objects from S3.

        :param object_keys: List of key names in S3.
        :return: Response from delete_objects call.
        """
        logger.info(f"Deleting {len(object_keys)} objects from S3")
        
        try:
            objects = [{'Key': key} for key in object_keys]
            response = self.s3_client.delete_objects(
                Bucket=self.bucket_name,
                Delete={'Objects': objects}
            )
            
            deleted = response.get('Deleted', [])
            errors = response.get('Errors', [])
            
            logger.info(f"Successfully deleted {len(deleted)} objects")
            if errors:
                logger.warning(f"Failed to delete {len(errors)} objects")
            
            return response
        except ClientError as error:
            logger.error(
                f"Failed to delete objects - Bucket: {self.bucket_name} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def copy_object(
        self,
        source_key: str,
        destination_key: str,
        source_bucket: Optional[str] = None,
        extra_args: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Copy an object within S3.

        :param source_key: Source object key.
        :param destination_key: Destination object key.
        :param source_bucket: Source bucket (defaults to current bucket).
        :param extra_args: Extra arguments to pass to copy_object.
        :return: S3 path of the copied object.
        """
        source_bucket = source_bucket or self.bucket_name
        source = f"{source_bucket}/{source_key}"
        destination_path = f"s3://{self.bucket_name}/{destination_key}"
        
        logger.info(f"Copying object from {source} to {destination_path}")
        
        try:
            copy_args = {
                'CopySource': source,
                'Bucket': self.bucket_name,
                'Key': destination_key
            }
            
            if extra_args:
                copy_args.update(extra_args)
            
            self.s3_client.copy_object(**copy_args)
            logger.info("Object copied successfully")
            return destination_path
        except ClientError as error:
            logger.error(
                f"Failed to copy object - Source: {source} | Destination: {destination_path} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def object_exists(self, object_key: str) -> bool:
        """
        Check if an object exists.

        :param object_key: Key name in S3.
        :return: True if object exists, False otherwise.
        """
        try:
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=object_key
            )
            return True
        except ClientError as error:
            if error.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(
                    f"Error checking object existence - Bucket: {self.bucket_name} | Key: {object_key} | "
                    f"Error: {error.response['Error']['Code']}"
                )
                raise error

    def get_object_metadata(self, object_key: str) -> Dict[str, Any]:
        """
        Get object metadata without downloading the content.

        :param object_key: Key name in S3.
        :return: Object metadata.
        """
        logger.info(f"Getting metadata for object: s3://{self.bucket_name}/{object_key}")
        
        try:
            response = self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=object_key
            )
            
            # Remove the ResponseMetadata
            metadata = dict(response)
            metadata.pop('ResponseMetadata', None)
            
            logger.info("Object metadata retrieved successfully")
            return metadata
        except ClientError as error:
            logger.error(
                f"Failed to get object metadata - Bucket: {self.bucket_name} | Key: {object_key} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def get_presigned_url(
        self,
        object_key: str,
        expiration: int = 3600,
        http_method: str = 'get_object'
    ) -> str:
        """
        Generate a presigned URL for an object.

        :param object_key: Key name in S3.
        :param expiration: Expiration time in seconds.
        :param http_method: HTTP method (get_object, put_object, etc.).
        :return: Presigned URL.
        """
        logger.info(f"Generating presigned URL for object: s3://{self.bucket_name}/{object_key}")
        
        try:
            url = self.s3_client.generate_presigned_url(
                http_method,
                Params={
                    'Bucket': self.bucket_name,
                    'Key': object_key
                },
                ExpiresIn=expiration
            )
            logger.info("Presigned URL generated successfully")
            return url
        except ClientError as error:
            logger.error(
                f"Failed to generate presigned URL - Bucket: {self.bucket_name} | Key: {object_key} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def set_bucket_policy(self, policy: Union[Dict[str, Any], str]) -> None:
        """
        Set the bucket policy.

        :param policy: Bucket policy as dict or JSON string.
        """
        logger.info(f"Setting bucket policy for bucket: {self.bucket_name}")
        
        try:
            if isinstance(policy, dict):
                policy = json.dumps(policy)
            
            self.s3_client.put_bucket_policy(
                Bucket=self.bucket_name,
                Policy=policy
            )
            logger.info("Bucket policy set successfully")
        except ClientError as error:
            logger.error(
                f"Failed to set bucket policy - Bucket: {self.bucket_name} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def get_bucket_policy(self) -> Dict[str, Any]:
        """
        Get the bucket policy.

        :return: Bucket policy as dict.
        """
        logger.info(f"Getting bucket policy for bucket: {self.bucket_name}")
        
        try:
            response = self.s3_client.get_bucket_policy(Bucket=self.bucket_name)
            policy = json.loads(response['Policy'])
            logger.info("Bucket policy retrieved successfully")
            return policy
        except ClientError as error:
            if error.response['Error']['Code'] == 'NoSuchBucketPolicy':
                logger.info("No bucket policy found")
                return {}
            else:
                logger.error(
                    f"Failed to get bucket policy - Bucket: {self.bucket_name} | "
                    f"Error: {error.response['Error']['Code']} | "
                    f"Message: {error.response['Error']['Message']}"
                )
                raise error
                      
    def list_objects(
        self,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        max_keys: int = 1000,
        max_pages: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        List objects in the bucket with pagination.

        :param prefix: Prefix to filter objects.
        :param delimiter: Delimiter for grouping keys.
        :param max_keys: Maximum number of keys per request.
        :param max_pages: Maximum number of pages to retrieve (None for all).
        :return: List of object metadata.
        """
        logger.info(f"Listing objects in bucket {self.bucket_name}")
        all_objects = []
        page_count = 0
        
        try:
            kwargs = {
                'Bucket': self.bucket_name,
                'MaxKeys': max_keys
            }
            
            if prefix:
                kwargs['Prefix'] = prefix
            if delimiter:
                kwargs['Delimiter'] = delimiter
            
            # Create paginator
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(**kwargs)
            
            for page in pages:
                page_count += 1
                objects = page.get('Contents', [])
                all_objects.extend(objects)
                
                logger.debug(f"Page {page_count}: Retrieved {len(objects)} objects")
                
                # Check if we've reached max pages
                if max_pages and page_count >= max_pages:
                    logger.info(f"Reached maximum page limit of {max_pages}")
                    break
            
            logger.info(f"Found {len(all_objects)} objects across {page_count} pages")
            return all_objects
        except ClientError as error:
            logger.error(
                f"Failed to list objects - Bucket: {self.bucket_name} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def list_objects_with_metadata(
        self,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        max_keys: int = 1000,
        max_pages: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        List objects with additional metadata about the listing.

        :param prefix: Prefix to filter objects.
        :param delimiter: Delimiter for grouping keys.
        :param max_keys: Maximum number of keys per request.
        :param max_pages: Maximum number of pages to retrieve (None for all).
        :return: Dict containing objects, common prefixes, and metadata.
        """
        logger.info(f"Listing objects with metadata in bucket {self.bucket_name}")
        all_objects = []
        all_common_prefixes = []
        page_count = 0
        total_objects_found = 0
        
        try:
            kwargs = {
                'Bucket': self.bucket_name,
                'MaxKeys': max_keys
            }
            
            if prefix:
                kwargs['Prefix'] = prefix
            if delimiter:
                kwargs['Delimiter'] = delimiter
            
            # Create paginator
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(**kwargs)
            
            for page in pages:
                page_count += 1
                objects = page.get('Contents', [])
                common_prefixes = page.get('CommonPrefixes', [])
                
                all_objects.extend(objects)
                all_common_prefixes.extend(common_prefixes)
                total_objects_found += len(objects)
                
                logger.debug(
                    f"Page {page_count}: Retrieved {len(objects)} objects, "
                    f"{len(common_prefixes)} common prefixes"
                )
                
                # Check if we've reached max pages
                if max_pages and page_count >= max_pages:
                    logger.info(f"Reached maximum page limit of {max_pages}")
                    break
            
            result = {
                'objects': all_objects,
                'common_prefixes': all_common_prefixes,
                'metadata': {
                    'total_objects': total_objects_found,
                    'total_pages': page_count,
                    'bucket': self.bucket_name,
                    'prefix': prefix,
                    'delimiter': delimiter
                }
            }
            
            logger.info(
                f"Found {total_objects_found} objects and {len(all_common_prefixes)} "
                f"common prefixes across {page_count} pages"
            )
            return result
        except ClientError as error:
            logger.error(
                f"Failed to list objects with metadata - Bucket: {self.bucket_name} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def list_objects_by_last_modified(
        self,
        prefix: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_keys: int = 1000,
        max_pages: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        List objects filtered by last modified date.

        :param prefix: Prefix to filter objects.
        :param start_date: Start date in ISO format (e.g., "2023-01-01").
        :param end_date: End date in ISO format (e.g., "2023-12-31").
        :param max_keys: Maximum number of keys per request.
        :param max_pages: Maximum number of pages to retrieve (None for all).
        :return: List of object metadata within the date range.
        """
        from datetime import datetime
        
        logger.info(
            f"Listing objects by last modified date - "
            f"Start: {start_date}, End: {end_date}"
        )
        
        # Get all objects first
        all_objects = self.list_objects(
            prefix=prefix,
            max_keys=max_keys,
            max_pages=max_pages
        )
        
        # Filter by date range if specified
        filtered_objects = []
        for obj in all_objects:
            last_modified = obj.get('LastModified')
            if last_modified:
                if start_date:
                    if last_modified.date() < datetime.fromisoformat(start_date).date():
                        continue
                if end_date:
                    if last_modified.date() > datetime.fromisoformat(end_date).date():
                        continue
                filtered_objects.append(obj)
        
        logger.info(f"Found {len(filtered_objects)} objects in date range")
        return filtered_objects

    def list_objects_by_size(
        self,
        prefix: Optional[str] = None,
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
        max_keys: int = 1000,
        max_pages: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        List objects filtered by size.

        :param prefix: Prefix to filter objects.
        :param min_size: Minimum size in bytes.
        :param max_size: Maximum size in bytes.
        :param max_keys: Maximum number of keys per request.
        :param max_pages: Maximum number of pages to retrieve (None for all).
        :return: List of object metadata within the size range.
        """
        logger.info(f"Listing objects by size - Min: {min_size}, Max: {max_size}")
        
        # Get all objects first
        all_objects = self.list_objects(
            prefix=prefix,
            max_keys=max_keys,
            max_pages=max_pages
        )
        
        # Filter by size range if specified
        filtered_objects = []
        for obj in all_objects:
            size = obj.get('Size', 0)
            if min_size is not None and size < min_size:
                continue
            if max_size is not None and size > max_size:
                continue
            filtered_objects.append(obj)
        
        logger.info(f"Found {len(filtered_objects)} objects in size range")
        return filtered_objects

    def list_objects_recursively(
        self,
        prefix: Optional[str] = None,
        max_keys: int = 1000,
        max_pages: Optional[int] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        List objects recursively, organizing by directory structure.

        :param prefix: Prefix to filter objects.
        :param max_keys: Maximum number of keys per request.
        :param max_pages: Maximum number of pages to retrieve (None for all).
        :return: Dict with directory structure.
        """
        logger.info(f"Listing objects recursively in bucket {self.bucket_name}")
        
        # Get objects with delimiter first to find directories
        result = self.list_objects_with_metadata(
            prefix=prefix,
            delimiter='/',
            max_keys=max_keys,
            max_pages=max_pages
        )
        
        directory_structure = {
            'files': result['objects'],
            'directories': {}
        }
        
        # Process common prefixes (directories)
        for common_prefix in result['common_prefixes']:
            prefix_key = common_prefix['Prefix']
            logger.debug(f"Processing directory: {prefix_key}")
            
            # Recursively get contents of this directory
            subdir_result = self.list_objects_recursively(
                prefix=prefix_key,
                max_keys=max_keys,
                max_pages=max_pages
            )
            
            directory_structure['directories'][prefix_key] = subdir_result
        
        return directory_structure
    
    def list_objects_advanced(
        self,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        max_keys: int = 1000,
        max_pages: Optional[int] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        List objects with advanced filtering options.

        :param prefix: Prefix to filter objects.
        :param delimiter: Delimiter for grouping keys.
        :param max_keys: Maximum number of keys per request.
        :param max_pages: Maximum number of pages to retrieve (None for all).
        :param filters: Dict with filter criteria (e.g., 'extension', 'min_size', 'max_size', 'date_range').
        :return: Dict containing filtered objects and metadata.
        """
        logger.info(f"Listing objects with advanced filters in bucket {self.bucket_name}")
        
        # Get all objects first
        all_objects = self.list_objects(
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
            max_pages=max_pages
        )
        
        if not filters:
            return {
                'objects': all_objects,
                'metadata': {
                    'total_count': len(all_objects),
                    'bucket': self.bucket_name
                }
            }
        
        filtered_objects = []
        for obj in all_objects:
            # Apply filters
            if not _apply_object_filters(obj, filters):
                continue
            filtered_objects.append(obj)
        
        result = {
            'objects': filtered_objects,
            'metadata': {
                'total_count': len(filtered_objects),
                'original_count': len(all_objects),
                'bucket': self.bucket_name,
                'filters_applied': filters
            }
        }
        
        logger.info(
            f"Applied filters: {len(filtered_objects)} objects out of {len(all_objects)} "
            f"matched the criteria"
        )
        return result


    def _apply_object_filters(obj: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        """
        Apply filters to object metadata.

        :param obj: Object metadata.
        :param filters: Filter criteria.
        :return: True if object passes all filters.
        """
        # Extension filter
        if 'extension' in filters:
            expected_ext = filters['extension']
            key = obj.get('Key', '')
            if not key.lower().endswith(f".{expected_ext.lower()}"):
                return False
        
        # Size filters
        size = obj.get('Size', 0)
        if 'min_size' in filters and size < filters['min_size']:
            return False
        if 'max_size' in filters and size > filters['max_size']:
            return False
        
        # Date filter
        if 'date_range' in filters:
            from datetime import datetime
            last_modified = obj.get('LastModified')
            if last_modified:
                date_range = filters['date_range']
                start_date = date_range.get('start')
                end_date = date_range.get('end')
                
                if start_date and last_modified.date() < datetime.fromisoformat(start_date).date():
                    return False
                if end_date and last_modified.date() > datetime.fromisoformat(end_date).date():
                    return False
        
        # Key pattern filter
        if 'key_pattern' in filters:
            import re
            pattern = filters['key_pattern']
            key = obj.get('Key', '')
            if not re.search(pattern, key):
                return False
        
        return True