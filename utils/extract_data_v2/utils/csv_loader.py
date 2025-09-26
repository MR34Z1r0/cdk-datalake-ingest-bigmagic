# -*- coding: utf-8 -*-
import csv
import boto3
from io import StringIO
from typing import List, Dict, Any
import os

class CSVConfigLoader:
    """Load configuration from CSV files (local or S3)"""
    
    @staticmethod
    def load_from_s3(s3_path: str) -> List[Dict[str, Any]]:
        """Load CSV file from S3 and return as list of dictionaries"""
        s3_client = boto3.client('s3')
        bucket = s3_path.split('/')[2]
        key = '/'.join(s3_path.split('/')[3:])
        
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('latin-1')
        
        return CSVConfigLoader._parse_csv_content(content)
    
    @staticmethod
    def load_from_local(file_path: str) -> List[Dict[str, Any]]:
        """Load CSV file from local filesystem"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        with open(file_path, mode='r', encoding='latin-1') as file:
            content = file.read()
        
        return CSVConfigLoader._parse_csv_content(content)
    
    @staticmethod
    def _parse_csv_content(content: str) -> List[Dict[str, Any]]:
        """Parse CSV content and return as list of dictionaries"""
        csv_data = []
        reader = csv.DictReader(StringIO(content), delimiter=';')
        
        for row in reader:
            # Clean up the row data
            cleaned_row = {}
            for key, value in row.items():
                if key:  # Skip empty keys
                    cleaned_row[key.strip()] = value.strip() if value else ''
            csv_data.append(cleaned_row)
        
        return csv_data
    
    @staticmethod
    def find_config_by_criteria(data: List[Dict[str, Any]], **criteria) -> Dict[str, Any]:
        """Find configuration by multiple criteria"""
        for row in data:
            match = True
            for key, value in criteria.items():
                if row.get(key, '').upper() != str(value).upper():
                    match = False
                    break
            if match:
                return row
        
        raise ValueError(f"Configuration not found with criteria: {criteria}")
    
    @staticmethod
    def filter_configs(data: List[Dict[str, Any]], **criteria) -> List[Dict[str, Any]]:
        """Filter configurations by criteria"""
        filtered = []
        for row in data:
            match = True
            for key, value in criteria.items():
                if row.get(key, '').upper() != str(value).upper():
                    match = False
                    break
            if match:
                filtered.append(row)
        
        return filtered