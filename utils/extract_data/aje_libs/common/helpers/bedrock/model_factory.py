# src/aje_libs/bd/helpers/bedrock/model_factory.py
from typing import Dict, Type, Optional, List, Any
from .base_model import BaseBedrockModel
from .models.meta_model import MetaModel
from .models.anthropic_model import AnthropicModel
from .models.amazon_model import AmazonModel
from .models.nova_model import NovaModel
from .models.cohere_model import CohereModel
from .models.ai21_model import AI21Model
from .models.deepseek_model import DeepSeekModel
from .models.model_enums import BedrockModel

class ModelFactory:
    """Factory para crear instancias de modelos."""
    
    _models: Dict[str, Type[BaseBedrockModel]] = {
        'meta': MetaModel,
        'anthropic': AnthropicModel,
        'amazon': AmazonModel,
        'nova': NovaModel,
        'cohere': CohereModel,
        'ai21': AI21Model,
        'deepseek': DeepSeekModel
    }
    
    @classmethod
    def get_model(cls, model_id: str) -> BaseBedrockModel:
        """
        Obtiene una instancia del modelo basado en el model_id.
        
        :param model_id: ID del modelo
        :return: Instancia del modelo
        """
        provider = cls.detect_provider(model_id)
        model_class = cls._models.get(provider)
        
        if not model_class:
            raise ValueError(f"Proveedor no soportado: {provider}")
        
        return model_class()
    
    @classmethod
    def get_model_from_enum(cls, model: BedrockModel) -> BaseBedrockModel:
        """Obtiene una instancia del modelo usando el enum."""
        provider = BedrockModel.get_provider(model)
        model_class = cls._models.get(provider)
        
        if not model_class:
            raise ValueError(f"Proveedor no soportado: {provider}")
        
        return model_class()
    
    @staticmethod
    def detect_provider(model_id: str) -> str:
        """
        Detecta el proveedor del modelo basado en su ID.
        
        :param model_id: ID del modelo
        :return: Proveedor del modelo
        """
        model_id = model_id.lower()
        
        # Primero intentar con el enum
        try:
            for model in BedrockModel:
                if model.value.lower() == model_id:
                    return BedrockModel.get_provider(model)
        except:
            pass
        
        # Patterns de respaldo
        patterns = {
            'meta': ['meta', 'llama'],
            'anthropic': ['anthropic', 'claude'],
            'amazon': ['amazon', 'titan'],
            'nova': ['nova'],
            'cohere': ['cohere'],
            'ai21': ['ai21', 'jurassic'],
            'deepseek': ['deepseek', 'ds-']
        }
        
        for provider, keywords in patterns.items():
            if any(keyword in model_id for keyword in keywords):
                return provider
        
        return 'unknown'