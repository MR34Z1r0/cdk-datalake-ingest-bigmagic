# src/aje_libs/bd/helpers/bedrock/models/ai21_model.py
from typing import Dict, Any, List
from ..base_model import BaseBedrockModel

class AI21Model(BaseBedrockModel):
    """Implementación para modelos AI21."""
    
    def format_prompt(self, prompt: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "prompt": prompt,
            "temperature": parameters.get("temperature", 0.7),
            "topP": parameters.get("top_p", 0.9),
            "maxTokens": parameters.get("max_tokens", 512),
            "stopSequences": parameters.get("stop_sequences", [])
        }
    
    def parse_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        completions = response.get("completions", [{}])
        first_completion = completions[0] if completions else {}
        completion_data = first_completion.get("data", {})
        
        return {
            "text": completion_data.get("text", ""),
            "input_token_count": response.get("prompt", {}).get("tokens", {}).get("length", 0),
            "output_token_count": completion_data.get("tokens", {}).get("length", 0),
            "total_tokens": 0  # AI21 no proporciona conteo total directo
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
        return ["text_generation"]