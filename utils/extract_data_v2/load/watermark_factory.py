# load/watermark_factory.py
from interfaces.watermark_interface import WatermarkStorageInterface
from .watermark_storage.dynamodb_watermark_storage import DynamoDBWatermarkStorage 
from .watermark_storage.csv_watermark_storage import CSVWatermarkStorage
from .watermark_storage.transactional_watermark_storage import TransactionalWatermarkStorage
from aje_libs.common.datalake_logger import DataLakeLogger

logger = DataLakeLogger.get_logger(__name__)

class WatermarkStorageFactory:
    """Factory para crear storage de watermarks"""
    
    @classmethod
    def create(cls, storage_type: str, enable_transactions: bool = True, **config) -> WatermarkStorageInterface:
        """
        Crea storage de watermarks
        
        Args:
            storage_type: 'dynamodb' o 'csv'
            enable_transactions: Si True, envuelve en TransactionalWatermarkStorage
            **config: Configuración específica
        """
        # Crear storage base
        if storage_type.lower() == 'dynamodb':
            base_storage = DynamoDBWatermarkStorage(
                table_name=config['table_name'],
                project_name=config['project_name']
            )
        elif storage_type.lower() == 'csv':
            base_storage = CSVWatermarkStorage(
                csv_file_path=config['csv_file_path'],
                project_name=config['project_name']
            )
        else:
            raise ValueError(f"Unsupported watermark storage type: {storage_type}")
        
        # Envolver en capa transaccional si está habilitado
        if enable_transactions:
            logger.info(f"✅ Creating transactional watermark storage")
            return TransactionalWatermarkStorage(
                base_storage=base_storage,
                project_name=config['project_name']
            )
        else:
            logger.info(f"ℹ️ Creating non-transactional watermark storage")
            return base_storage