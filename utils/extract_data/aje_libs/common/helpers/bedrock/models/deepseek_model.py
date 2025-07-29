# src/aje_libs/bd/helpers/bedrock/models/deepseek_model.py
from typing import Dict, Any, List
from ..base_model import BaseBedrockModel

class DeepSeekModel(BaseBedrockModel):
    """Implementación para modelos DeepSeek."""
    
    def format_prompt(self, prompt: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        # DeepSeek suele usar un formato similar a OpenAI/Mistral
        messages = [
            {"role": "user", "content": prompt}
        ]
        
        return {
            "messages": messages,
            "temperature": parameters.get("temperature", 0.7),
            "top_p": parameters.get("top_p", 0.9),
            "max_tokens": parameters.get("max_tokens", 1024),
            "stop": parameters.get("stop_sequences", []),
            "stream": parameters.get("stream", False)
        }
    
    def parse_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        # Extraer la respuesta según el formato de DeepSeek
        choices = response.get("choices", [])
        text = choices[0].get("message", {}).get("content", "") if choices else ""
        
        # Extraer información de tokens si está disponible
        usage = response.get("usage", {})
        
        return {
            "text": text,
            "input_token_count": usage.get("prompt_tokens", 0),
            "output_token_count": usage.get("completion_tokens", 0),
            "total_tokens": usage.get("total_tokens", 0),
            "model": response.get("model", ""),
            "finish_reason": choices[0].get("finish_reason", "") if choices else ""
        }
    
    @property
    def default_parameters(self) -> Dict[str, Any]:
        return {
            "temperature": 0.7,
            "top_p": 0.9,
            "max_tokens": 1024,
            "stop_sequences": [],
            "stream": False
        }
    
    @property
    def supported_features(self) -> List[str]:
        return ["text_generation", "conversation", "function_calling"]
    
    def format_function_call(self, messages: List[Dict[str, Any]], functions: List[Dict[str, Any]], function_call: str = "auto") -> Dict[str, Any]:
        """Formato específico para llamadas a funciones en DeepSeek."""
        request_body = {
            "messages": messages,
            "functions": functions,
            "function_call": function_call,
            "temperature": 0,  # Temperatura baja para function calling
            "max_tokens": 512
        }
        return request_body
    
    def parse_function_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """Parsea respuestas de function calling."""
        choices = response.get("choices", [])
        if not choices:
            return {"function_call": None, "text": ""}
        
        message = choices[0].get("message", {})
        function_call = message.get("function_call", {})
        
        if function_call:
            return {
                "function_call": {
                    "name": function_call.get("name", ""),
                    "arguments": function_call.get("arguments", "{}")
                },
                "text": message.get("content", "")
            }
        else:
            return {
                "function_call": None,
                "text": message.get("content", "")
            }