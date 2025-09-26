import os
from typing import Dict, List, Optional, Union
from ...common.logger import custom_logger

logger = custom_logger(__name__)

class DocumentProcessor:
    """Procesador de documentos que utiliza los helpers específicos"""
    
    def process_document(self, file_path: str) -> Optional[str]:
        """
        Procesa un documento según su tipo.
        
        :param file_path: Ruta al archivo
        :return: Texto extraído o None si el formato no es soportado
        """
        file_extension = self.get_file_extension(file_path)
        
        try:
            if file_extension == 'pdf':
                from .pdf_helper import PDFHelper
                self.pdf_helper = PDFHelper()
                return self.pdf_helper.extract_text(file_path)
            elif file_extension == 'pptx' or file_extension == 'ppt':
                from .ppt_helper import PPTXHelper
                self.pptx_helper = PPTXHelper()
                return self.pptx_helper.extract_text(file_path)
            elif file_extension == 'docx' or file_extension == 'doc':
                from .doc_helper import DOCXHelper
                self.docx_helper = DOCXHelper()
                return self.docx_helper.extract_text(file_path)
            elif file_extension == 'xlsx' or file_extension == 'xls':
                from .xls_helper import ExcelHelper
                self.excel_helper = ExcelHelper()
                return self.process_excel_to_text(file_path)
            else:
                logger.warning(f"Formato no soportado: {file_extension}")
                return None
        except Exception as e:
            logger.error(f"Error procesando documento: {e}")
            raise e
    
    def process_excel_to_text(self, file_path: str) -> str:
        """
        Convierte datos de Excel a texto.
        
        :param file_path: Ruta al archivo Excel
        :return: Texto con datos de Excel
        """
        try:
            all_data = self.excel_helper.extract_all_data(file_path)
            text_parts = []
            
            for sheet_name, sheet_data in all_data.items():
                text_parts.append(f"Sheet: {sheet_name}")
                for row_idx, row in enumerate(sheet_data):
                    row_text = " | ".join(str(cell) if cell is not None else "" for cell in row)
                    text_parts.append(f"Row {row_idx + 1}: {row_text}")
                text_parts.append("")  # Espacio entre hojas
            
            return "\n".join(text_parts)
        except Exception as e:
            logger.error(f"Error convirtiendo Excel a texto: {e}")
            raise e
    
    @staticmethod
    def get_file_extension(file_path: str) -> str:
        """
        Obtiene la extensión del archivo.
        
        :param file_path: Ruta al archivo
        :return: Extensión del archivo
        """
        return os.path.splitext(file_path)[1].lower().replace(".", "")
    
    def process_by_chunks(self, file_path: str, chunk_size: int = 1000) -> List[str]:
        """
        Procesa un documento dividiéndolo en chunks.
        
        :param file_path: Ruta al archivo
        :param chunk_size: Tamaño de cada chunk
        :return: Lista de chunks de texto
        """
        try:
            text = self.process_document(file_path)
            if not text:
                return []
            
            chunks = []
            words = text.split()
            current_chunk = []
            current_size = 0
            
            for word in words:
                current_chunk.append(word)
                current_size += len(word) + 1  # +1 for space
                
                if current_size >= chunk_size:
                    chunks.append(" ".join(current_chunk))
                    current_chunk = []
                    current_size = 0
            
            if current_chunk:
                chunks.append(" ".join(current_chunk))
            
            logger.info(f"Documento dividido en {len(chunks)} chunks")
            return chunks
        except Exception as e:
            logger.error(f"Error dividiendo documento en chunks: {e}")
            raise e