# src/aje_libs/bd/helpers/bedrock/models/cohere_model.py
from typing import Dict, Any, List
from ..base_model import BaseBedrockModel

class CohereModel(BaseBedrockModel):
    """Implementación para modelos Cohere."""
    
    def format_prompt(self, prompt: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "prompt": prompt,
            "temperature": parameters.get("temperature", 0.7),
            "p": parameters.get("top_p", 0.9),
            "max_tokens": parameters.get("max_tokens", 512),
            "stop_sequences": parameters.get("stop_sequences", [])
        }
    
    def parse_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        generations = response.get("generations", [{}])
        first_gen = generations[0] if generations else {}
        
        meta = response.get("meta", {})
        tokens_info = meta.get("tokens", {})
        
        return {
            "text": first_gen.get("text", ""),
            "input_token_count": tokens_info.get("input", 0),
            "output_token_count": tokens_info.get("generated", 0),
            "total_tokens": tokens_info.get("total", 0)
        }
    
    @property
    def default_parameters(self) -> Dict[str, Any]:
        return {
            "temperature": 0.7,
            "top_p": 0.9,
            "max_tokens": 512,
            "stop_sequences": []
        }
    
    @property
    def supported_features(self) -> List[str]:
        return ["text_generation", "embeddings"]