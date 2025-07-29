# src/aje_libs/bd/helpers/bedrock/base_model.py
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List

class BaseBedrockModel(ABC):
    """Clase base para todos los modelos de Bedrock."""
    
    @abstractmethod
    def format_prompt(self, prompt: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Formatea el prompt según el modelo."""
        pass
    
    @abstractmethod
    def parse_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """Parsea la respuesta del modelo."""
        pass
    
    @property
    @abstractmethod
    def default_parameters(self) -> Dict[str, Any]:
        """Parámetros por defecto del modelo."""
        pass
    
    @property
    @abstractmethod
    def supported_features(self) -> List[str]:
        """Características soportadas por el modelo."""
        pass
    
    # Métodos opcionales para function calling
    def format_function_call(self, messages: List[Dict[str, Any]], functions: List[Dict[str, Any]], function_call: str = "auto") -> Dict[str, Any]:
        """Formato para llamadas a funciones (opcional)."""
        raise NotImplementedError("Este modelo no soporta function calling")
    
    def parse_function_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """Parsea respuestas de function calling (opcional)."""
        raise NotImplementedError("Este modelo no soporta function calling")