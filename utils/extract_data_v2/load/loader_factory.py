# -*- coding: utf-8 -*-
from typing import Dict, Type, Any
from interfaces.loader_interface import LoaderInterface
from load.loaders.s3_loader import S3Loader
from load.formatters.parquet_formatter import ParquetFormatter
from load.formatters.csv_formatter import CSVFormatter
from exceptions.custom_exceptions import ConfigurationError

class LoaderFactory:
    """Factory to create appropriate loader instances"""
    
    _loaders: Dict[str, Type[LoaderInterface]] = {
        's3': S3Loader,
        # Add more loaders here as they are implemented
        # 'local': LocalFileLoader,
        # 'database': DatabaseLoader,
        # 'azure': AzureBlobLoader,
    }
    
    _formatters = {
        'parquet': ParquetFormatter,
        'csv': CSVFormatter,
        # 'json': JSONFormatter,
    }
    
    @classmethod
    def create(cls, loader_type: str, output_format: str, **config) -> LoaderInterface:
        """
        Create appropriate loader with formatter
        
        Args:
            loader_type: Type of loader ('s3', 'local', etc.)
            output_format: Output format ('parquet', 'csv', 'json')
            **config: Loader-specific configuration
            
        Returns:
            Configured loader instance with formatter
            
        Raises:
            ConfigurationError: If loader type or format is not supported
        """
        loader_type_lower = loader_type.lower()
        format_lower = output_format.lower()
        
        # Validate loader type
        if loader_type_lower not in cls._loaders:
            available_loaders = ', '.join(cls._loaders.keys())
            raise ConfigurationError(
                f"Unsupported loader type '{loader_type}'. "
                f"Available types: {available_loaders}"
            )
        
        # Validate format
        if format_lower not in cls._formatters:
            available_formats = ', '.join(cls._formatters.keys())
            raise ConfigurationError(
                f"Unsupported output format '{output_format}'. "
                f"Available formats: {available_formats}"
            )
        
        # Create formatter
        formatter_class = cls._formatters[format_lower]
        formatter = formatter_class(**config.get('formatter_options', {}))
        
        # Create loader
        loader_class = cls._loaders[loader_type_lower]
        loader = loader_class(**config)
        
        # Set formatter on loader
        if hasattr(loader, 'set_formatter'):
            loader.set_formatter(formatter)
        
        return loader
    
    @classmethod
    def register_loader(cls, loader_type: str, loader_class: Type[LoaderInterface]):
        """Register a new loader type"""
        cls._loaders[loader_type.lower()] = loader_class
    
    @classmethod
    def register_formatter(cls, format_type: str, formatter_class):
        """Register a new formatter type"""
        cls._formatters[format_type.lower()] = formatter_class
    
    @classmethod
    def get_supported_loaders(cls) -> list:
        """Get list of supported loader types"""
        return list(cls._loaders.keys())
    
    @classmethod
    def get_supported_formats(cls) -> list:
        """Get list of supported output formats"""
        return list(cls._formatters.keys())