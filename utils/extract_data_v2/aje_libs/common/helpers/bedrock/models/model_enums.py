# src/aje_libs/bd/helpers/bedrock/models/model_enums.py
from enum import Enum
from typing import Dict

class BedrockModel(Enum):
    """Enum con todos los modelos disponibles en Bedrock."""
    
    # Meta (Llama) Models
    META_LLAMA3_2_3B_INSTRUCT = "us.meta.llama3-2-3b-instruct-v1:0"
    META_LLAMA3_2_11B_INSTRUCT = "us.meta.llama3-2-11b-instruct-v1:0"
    META_LLAMA3_2_90B_INSTRUCT = "us.meta.llama3-2-90b-instruct-v1:0"
    META_LLAMA3_70B_INSTRUCT = "us.meta.llama3-70b-instruct-v1:0"
    META_LLAMA3_8B_INSTRUCT = "us.meta.llama3-8b-instruct-v1:0"
    
    # Anthropic (Claude) Models
    CLAUDE_3_HAIKU_20240307 = "anthropic.claude-3-haiku-20240307-v1:0"
    CLAUDE_3_SONNET_20240229 = "anthropic.claude-3-sonnet-20240229-v1:0"
    CLAUDE_3_OPUS_20240229 = "anthropic.claude-3-opus-20240229-v1:0"
    CLAUDE_INSTANT_1_2 = "anthropic.claude-instant-v1"
    
    # Amazon (Titan) Models
    TITAN_TEXT_PREMIER = "us.amazon.titan-text-premier-v1:0"
    TITAN_TEXT_EXPRESS = "us.amazon.titan-text-express-v1"
    TITAN_TEXT_LITE = "us.amazon.titan-text-lite-v1"
    TITAN_EMBED_TEXT_V2 = "us.amazon.titan-embed-text-v2:0"
    TITAN_EMBED_TEXT_V1 = "us.amazon.titan-embed-text-v1"
    
    # Amazon Nova Models (2024 releases)
    # Text Models
    NOVA_MICRO = "us.amazon.nova-micro-v1:0"
    NOVA_LITE = "us.amazon.nova-lite-v1:0"
    NOVA_PRO = "us.amazon.nova-pro-v1:0"
    
    # Embedding Models
    NOVA_EMBED_V1 = "amazon.nova-embed-v1"
    
    # Cohere Models
    COHERE_COMMAND_TEXT_V14 = "cohere.command-text-v14"
    COHERE_COMMAND_LIGHT_TEXT_V14 = "cohere.command-light-text-v14"
    COHERE_EMBED_ENGLISH_V3 = "cohere.embed-english-v3"
    COHERE_EMBED_MULTILINGUAL_V3 = "cohere.embed-multilingual-v3"
    
    # AI21 Labs Models
    AI21_JAMBA_INSTRUCT = "ai21.jamba-instruct-v1:0"
    AI21_J2_ULTRA = "ai21.j2-ultra-v1"
    AI21_J2_MID = "ai21.j2-mid-v1"
    
    # DeepSeek Models
    DEEPSEEK_DISTILL_LLAMA_8B = "deepseek.deepseek-r1-distill-llama-8b"
    
    @classmethod
    def get_provider(cls, model: 'BedrockModel') -> str:
        """Obtiene el proveedor de un modelo."""
        model_id = model.value.lower()
        if "meta" in model_id or "llama" in model_id:
            return "meta"
        elif "anthropic" in model_id or "claude" in model_id:
            return "anthropic"
        elif "amazon" in model_id or "titan" in model_id or "nova" in model_id:
            return "amazon" if "titan" in model_id else "nova"
        elif "cohere" in model_id:
            return "cohere"
        elif "ai21" in model_id:
            return "ai21"
        elif "deepseek" in model_id:
            return "deepseek"
        else:
            return "unknown"
    
    @classmethod
    def get_all_by_provider(cls, provider: str) -> Dict[str, 'BedrockModel']:
        """Obtiene todos los modelos de un proveedor específico."""
        provider_models = {}
        for model in cls:
            if cls.get_provider(model) == provider:
                provider_models[model.name] = model
        return provider_models

class BedrockModelCategory(Enum):
    """Categorías de modelos en Bedrock."""
    TEXT_GENERATION = "text-generation"
    EMBEDDING = "embedding"
    IMAGE_GENERATION = "image-generation"
    MULTIMODAL = "multimodal"
    
    @staticmethod
    def get_model_category(model: BedrockModel) -> 'BedrockModelCategory':
        """Determina la categoría de un modelo."""
        model_id = model.value.lower()
        if "embed" in model_id:
            return BedrockModelCategory.EMBEDDING
        elif any(keyword in model_id for keyword in ["image", "vision", "multimodal"]):
            return BedrockModelCategory.MULTIMODAL
        else:
            return BedrockModelCategory.TEXT_GENERATION