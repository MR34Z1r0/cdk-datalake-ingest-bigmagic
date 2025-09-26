# -*- coding: utf-8 -*-
from typing import Dict, Type
from interfaces.monitor_interface import MonitorInterface
from monitoring.monitors.dynamodb_monitor import DynamoDBMonitor
from exceptions.custom_exceptions import ConfigurationError

class MonitorFactory:
    """Factory to create appropriate monitor instances"""
    
    _monitors: Dict[str, Type[MonitorInterface]] = {
        'dynamodb': DynamoDBMonitor,
        # Add more monitors here as they are implemented
        # 'cloudwatch': CloudWatchMonitor,
        # 'file': FileMonitor,
    }
    
    @classmethod
    def create(cls, monitor_type: str, **config) -> MonitorInterface:
        """
        Create appropriate monitor instance
        
        Args:
            monitor_type: Type of monitor ('dynamodb', 'cloudwatch', etc.)
            **config: Monitor-specific configuration
            
        Returns:
            Configured monitor instance
            
        Raises:
            ConfigurationError: If monitor type is not supported
        """
        monitor_type_lower = monitor_type.lower()
        
        if monitor_type_lower not in cls._monitors:
            available_monitors = ', '.join(cls._monitors.keys())
            raise ConfigurationError(
                f"Unsupported monitor type '{monitor_type}'. "
                f"Available types: {available_monitors}"
            )
        
        monitor_class = cls._monitors[monitor_type_lower]
        return monitor_class(**config)
    
    @classmethod
    def register_monitor(cls, monitor_type: str, monitor_class: Type[MonitorInterface]):
        """Register a new monitor type"""
        cls._monitors[monitor_type.lower()] = monitor_class
    
    @classmethod
    def get_supported_types(cls) -> list:
        """Get list of supported monitor types"""
        return list(cls._monitors.keys())