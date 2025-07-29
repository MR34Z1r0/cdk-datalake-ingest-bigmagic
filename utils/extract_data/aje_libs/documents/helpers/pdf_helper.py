# src/aje_libs/bd/helpers/pdf_helper.py
import PyPDF2
import io
from typing import Dict, List, Optional
from ...common.logger import custom_logger

logger = custom_logger(__name__)

class PDFHelper:
    """Helper para procesar archivos PDF"""
    
    def __init__(self):
        pass
    
    def extract_text(self, file_path: str) -> str:
        """
        Extrae texto de un archivo PDF.
        
        :param file_path: Ruta al archivo PDF
        :return: Texto extraído
        """
        try:
            text = ""
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                
                for page_num in range(len(pdf_reader.pages)):
                    page = pdf_reader.pages[page_num]
                    text += page.extract_text() + "\n"
            
            logger.info(f"Texto extraído exitosamente de PDF: {file_path}")
            return text
        except Exception as e:
            logger.error(f"Error extrayendo texto de PDF: {e}")
            raise e
    
    def extract_text_by_pages(self, file_path: str) -> List[str]:
        """
        Extrae texto de un PDF página por página.
        
        :param file_path: Ruta al archivo PDF
        :return: Lista de textos por página
        """
        try:
            pages = []
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                
                for page_num in range(len(pdf_reader.pages)):
                    page = pdf_reader.pages[page_num]
                    text = page.extract_text()
                    pages.append(text)
            
            logger.info(f"Extraído texto de {len(pages)} páginas")
            return pages
        except Exception as e:
            logger.error(f"Error extrayendo texto por páginas: {e}")
            raise e
    
    def get_metadata(self, file_path: str) -> Dict[str, str]:
        """
        Obtiene metadatos del archivo PDF.
        
        :param file_path: Ruta al archivo PDF
        :return: Diccionario con metadatos
        """
        try:
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                metadata = pdf_reader.metadata or {}
                
                return {
                    'title': metadata.get('/Title', ''),
                    'author': metadata.get('/Author', ''),
                    'subject': metadata.get('/Subject', ''),
                    'creator': metadata.get('/Creator', ''),
                    'producer': metadata.get('/Producer', ''),
                    'pages': len(pdf_reader.pages)
                }
        except Exception as e:
            logger.error(f"Error obteniendo metadatos: {e}")
            raise e