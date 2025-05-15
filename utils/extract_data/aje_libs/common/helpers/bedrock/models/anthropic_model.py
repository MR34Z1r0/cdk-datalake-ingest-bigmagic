# src/aje_libs/bd/helpers/bedrock/models/anthropic_model.py
from typing import Dict, Any, List
from ..base_model import BaseBedrockModel

class AnthropicModel(BaseBedrockModel):
    """Implementación para modelos Anthropic (Claude)."""
    
    def format_prompt(self, prompt: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "prompt": f"\n\nHuman: {prompt}\n\nAssistant:",
            "temperature": parameters.get("temperature", 0.7),
            "top_p": parameters.get("top_p", 0.9),
            "max_tokens_to_sample": parameters.get("max_tokens", 512),
            "stop_sequences": parameters.get("stop_sequences", ["\n\nHuman:"])
        }
    
    def parse_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        usage = response.get("usage", {})
        return {
            "text": response.get("completion", ""),
            "input_token_count": usage.get("input_tokens", 0),
            "output_token_count": usage.get("output_tokens", 0),
            "total_tokens": usage.get("total_tokens", 0)
        }
    
    @property
    def default_parameters(self) -> Dict[str, Any]:
        return {
            "temperature": 0.7,
            "top_p": 0.9,
            "max_tokens": 512,
            "stop_sequences": ["\n\nHuman:"]
        }
    
    @property
    def supported_features(self) -> List[str]:
        return ["text_generation", "conversation"]