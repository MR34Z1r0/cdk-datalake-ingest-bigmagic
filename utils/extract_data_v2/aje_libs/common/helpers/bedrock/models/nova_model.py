# src/aje_libs/bd/helpers/bedrock/models/nova_model.py
from typing import Dict, Any, List
from ..base_model import BaseBedrockModel

class NovaModel(BaseBedrockModel):
    """Implementación para modelos Amazon Nova."""
    
    def format_prompt(self, prompt: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        # Nova usa formato de mensajes
        messages = [
            {"role": "user", "content": [{"text": prompt}]}
        ]
        
        return {
            "messages": messages,
            "inferenceConfig": {
                "maxTokens": parameters.get("max_tokens", 512),
                "temperature": parameters.get("temperature", 0.7),
                "topP": parameters.get("top_p", 0.9)
            }
        }
    
    def parse_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        # Nova response format
        output = response.get("output", {})
        message = output.get("message", {})
        content = message.get("content", [])
        
        text = content[0].get("text", "") if content else ""
        
        usage = response.get("usage", {})
        
        return {
            "text": text,
            "input_token_count": usage.get("inputTokens", 0),
            "output_token_count": usage.get("outputTokens", 0),
            "total_tokens": usage.get("inputTokens", 0) + usage.get("outputTokens", 0)
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
        return ["text_generation", "conversation", "multimodal"]