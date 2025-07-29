# src/aje_libs/bd/helpers/bedrock/models/amazon_model.py
from typing import Dict, Any, List
from ..base_model import BaseBedrockModel

class AmazonModel(BaseBedrockModel):
    """Implementación para modelos Amazon (Titan)."""
    
    def format_prompt(self, prompt: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "inputText": prompt,
            "textGenerationConfig": {
                "temperature": parameters.get("temperature", 0.7),
                "topP": parameters.get("top_p", 0.9),
                "maxTokenCount": parameters.get("max_tokens", 512),
                "stopSequences": parameters.get("stop_sequences", [])
            }
        }
    
    def parse_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        results = response.get("results", [])
        result = results[0] if results else {}
        return {
            "text": result.get("outputText", ""),
            "input_token_count": response.get("inputTextTokenCount", 0),
            "output_token_count": result.get("tokenCount", 0),
            "total_tokens": response.get("inputTextTokenCount", 0) + result.get("tokenCount", 0)
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