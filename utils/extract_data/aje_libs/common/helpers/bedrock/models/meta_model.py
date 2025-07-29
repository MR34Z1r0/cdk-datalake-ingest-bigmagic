# src/aje_libs/bd/helpers/bedrock/models/meta_model.py
from typing import Dict, Any, List
from ..base_model import BaseBedrockModel

class MetaModel(BaseBedrockModel):
    """Implementación para modelos Meta (Llama)."""
    
    def format_prompt(self, prompt: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "prompt": prompt,
            "temperature": parameters.get("temperature", 0.7),
            "top_p": parameters.get("top_p", 0.9),
            "max_gen_len": parameters.get("max_tokens", 512)
        }
    
    def parse_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "text": response.get("generation", ""),
            "input_token_count": response.get("prompt_token_count", 0),
            "output_token_count": response.get("generation_token_count", 0),
            "total_tokens": response.get("prompt_token_count", 0) + response.get("generation_token_count", 0)
        }
    
    @property
    def default_parameters(self) -> Dict[str, Any]:
        return {
            "temperature": 0.7,
            "top_p": 0.9,
            "max_tokens": 512
        }
    
    @property
    def supported_features(self) -> List[str]:
        if self.model_id == "us.meta.llama3-2-11b-instruct-v1:0" or self.model_id == "us.meta.llama3-2-90b-instruct-v1:0":
            return ["text_generation", "conversation", "multimodal"]
        else:
            return ["text_generation", "conversation"]