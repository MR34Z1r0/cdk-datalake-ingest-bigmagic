# load/watermark_storage/csv_watermark_storage.py
import csv
import json
import os
from typing import Optional, Dict, Any, List
from datetime import datetime
from interfaces.watermark_interface import WatermarkStorageInterface
from aje_libs.common.logger import custom_logger

class CSVWatermarkStorage(WatermarkStorageInterface):
    """Implementación CSV para watermarks (desarrollo/testing)"""
    
    def __init__(self, csv_file_path: str, project_name: str):
        self.logger = custom_logger(__name__)
        self.csv_file_path = csv_file_path
        self.project_name = project_name
        self._ensure_csv_exists()
    
    def _ensure_csv_exists(self):
        """Crear CSV si no existe"""
        if not os.path.exists(self.csv_file_path):
            os.makedirs(os.path.dirname(self.csv_file_path), exist_ok=True)
            with open(self.csv_file_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['project_name', 'table_name', 'column_name', 
                               'extracted_value', 'timestamp', 'metadata'])
    
    def get_last_extracted_value(self, table_name: str, column_name: str) -> Optional[str]:
        """Lee watermark del CSV"""
        try:
            watermarks = []
            with open(self.csv_file_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if (row['project_name'] == self.project_name and 
                        row['table_name'] == table_name and 
                        row['column_name'] == column_name):
                        watermarks.append(row)
            
            # Retornar el más reciente
            if watermarks:
                latest = max(watermarks, key=lambda x: x['timestamp'])
                return latest['extracted_value']
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Failed to read watermark from CSV: {e}")
            return None
    
    def set_last_extracted_value(self, table_name: str, column_name: str, 
                               value: str, metadata: Dict[str, Any] = None) -> bool:
        """Guarda watermark en CSV"""
        try:
            # Leer datos existentes
            existing_data = []
            if os.path.exists(self.csv_file_path):
                with open(self.csv_file_path, 'r') as f:
                    reader = csv.DictReader(f)
                    existing_data = list(reader)
            
            # Remover entrada anterior si existe
            existing_data = [row for row in existing_data if not (
                row['project_name'] == self.project_name and
                row['table_name'] == table_name and
                row['column_name'] == column_name
            )]
            
            # Agregar nueva entrada
            new_entry = {
                'project_name': self.project_name,
                'table_name': table_name,
                'column_name': column_name,
                'extracted_value': str(value),
                'timestamp': datetime.now().isoformat(),
                'metadata': json.dumps(metadata or {})
            }
            existing_data.append(new_entry)
            
            # Escribir de vuelta
            with open(self.csv_file_path, 'w', newline='') as f:
                if existing_data:
                    writer = csv.DictWriter(f, fieldnames=existing_data[0].keys())
                    writer.writeheader()
                    writer.writerows(existing_data)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save watermark to CSV: {e}")
            return False

    def get_extraction_history(self, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Obtiene el historial de extracciones"""
        try:
            watermarks = []
            with open(self.csv_file_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if (row['project_name'] == self.project_name and 
                        row['table_name'] == table_name):
                        watermarks.append({
                            'table_name': row['table_name'],
                            'column_name': row['column_name'],
                            'extracted_value': row['extracted_value'],
                            'timestamp': row['timestamp'],
                            'metadata': json.loads(row['metadata']) if row['metadata'] else {}
                        })
            
            # Ordenar por timestamp y limitar
            watermarks.sort(key=lambda x: x['timestamp'], reverse=True)
            return watermarks[:limit]
            
        except Exception as e:
            self.logger.warning(f"Failed to read extraction history: {e}")
            return []

    def cleanup_old_watermarks(self, days_to_keep: int = 90) -> int:
        """Limpia watermarks antiguos"""
        try:
            from datetime import datetime, timedelta
            
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            existing_data = []
            if os.path.exists(self.csv_file_path):
                with open(self.csv_file_path, 'r') as f:
                    reader = csv.DictReader(f)
                    existing_data = list(reader)
            
            # Filtrar registros antiguos
            cleaned_data = []
            removed_count = 0
            
            for row in existing_data:
                try:
                    row_timestamp = datetime.fromisoformat(row['timestamp'])
                    if row_timestamp > cutoff_date:
                        cleaned_data.append(row)
                    else:
                        removed_count += 1
                except (ValueError, KeyError):
                    # Mantener registros con timestamp inválido
                    cleaned_data.append(row)
            
            # Escribir datos limpios de vuelta
            if cleaned_data:
                with open(self.csv_file_path, 'w', newline='') as f:
                    if cleaned_data:
                        writer = csv.DictWriter(f, fieldnames=cleaned_data[0].keys())
                        writer.writeheader()
                        writer.writerows(cleaned_data)
            
            return removed_count
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup old watermarks: {e}")
            return 0        