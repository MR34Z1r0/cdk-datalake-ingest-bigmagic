# -*- coding: utf-8 -*-

class ExtractDataException(Exception):
    """Base exception for extract data operations"""
    pass

class ConfigurationError(ExtractDataException):
    """Configuration related errors"""
    pass

class ExtractionError(ExtractDataException):
    """Data extraction related errors"""
    pass

class LoadError(ExtractDataException):
    """Data loading related errors"""
    pass

class ConnectionError(ExtractDataException):
    """Database connection related errors"""
    pass

class ValidationError(ExtractDataException):
    """Data validation related errors"""
    pass