# load/watermark_storage/watermark_factory.py
from interfaces.watermark_interface import WatermarkStorageInterface
from .watermark_storage.dynamodb_watermark_storage import DynamoDBWatermarkStorage 
from .watermark_storage.csv_watermark_storage import CSVWatermarkStorage

class WatermarkStorageFactory:
    """Factory para crear storage de watermarks"""
    
    @classmethod
    def create(cls, storage_type: str, **config) -> WatermarkStorageInterface:
        """
        Crea storage de watermarks basado en tipo
        
        Args:
            storage_type: 'dynamodb', 'postgresql', 'csv'
            **config: Configuración específica del storage
        """
        if storage_type.lower() == 'dynamodb':
            return DynamoDBWatermarkStorage(
                table_name=config['table_name'],
                project_name=config['project_name']
            )
        
        elif storage_type.lower() == 'csv':
            return CSVWatermarkStorage(
                csv_file_path=config['csv_file_path'],
                project_name=config['project_name']
            )
        
        else:
            raise ValueError(f"Unsupported watermark storage type: {storage_type}")