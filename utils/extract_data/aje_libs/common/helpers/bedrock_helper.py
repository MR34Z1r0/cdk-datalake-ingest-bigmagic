# src/aje_libs/bd/helpers/bedrock_helper.py
import json
import boto3
from typing import Dict, Any, List, Optional, Union
from botocore.exceptions import ClientError
from ...common.logger import custom_logger
from .bedrock.model_factory import ModelFactory
from .bedrock.models.model_enums import BedrockModel, BedrockModelCategory

logger = custom_logger(__name__)

class BedrockHelper:
    """Helper principal para interactuar con diferentes modelos de AWS Bedrock."""
    
    def __init__(self, region_name: str = "us-west-2"):
        """
        Inicializa el cliente de Bedrock.
        
        :param region_name: Región de AWS
        """
        self.bedrock_client = boto3.client('bedrock-runtime', region_name=region_name)
        self.region_name = region_name
    
    def _get_model_id(self, model: Union[str, BedrockModel]) -> str:
        """Convierte el modelo a string si es un enum."""
        if isinstance(model, BedrockModel):
            return model.value
        return model
    
    def invoke_model(
        self,
        model: Union[str, BedrockModel],
        prompt: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Invoca un modelo de forma genérica.
        
        :param model: ID del modelo (string o enum BedrockModel)
        :param prompt: Prompt de entrada
        :param parameters: Parámetros de inferencia
        :return: Respuesta del modelo en formato normalizado
        """
        model_id = self._get_model_id(model)
        
        try:
            # CORREGIDO: Usa la instancia del modelo directamente
            model_instance = ModelFactory.get_model(model_id)
            
            final_parameters = model_instance.default_parameters.copy()
            if parameters:
                final_parameters.update(parameters)
            
            body = model_instance.format_prompt(prompt, final_parameters)
            
            logger.info(f"Invocando modelo {model_id}")
            response = self.bedrock_client.invoke_model(
                modelId=model_id,
                body=json.dumps(body)
            )
            
            response_body = response["body"].read()
            response_json = json.loads(response_body)
            
            return model_instance.parse_response(response_json)
            
        except ClientError as error:
            logger.error(f"Error invocando modelo: {error}")
            raise error
    
    def converse(
        self,
        model: Union[str, BedrockModel],
        messages: List[Dict[str, str]],
        system_prompt: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Usa la API de Converse para modelos que la soportan.
        
        :param model: ID del modelo (string o enum)
        :param messages: Lista de mensajes
        :param system_prompt: Prompt del sistema
        :param parameters: Parámetros de inferencia
        :return: Respuesta del modelo
        """
        model_id = self._get_model_id(model)
        
        try:
            # CORREGIDO: Usa la instancia del modelo directamente
            model_instance = ModelFactory.get_model(model_id)
            
            if "conversation" not in model_instance.supported_features:
                raise ValueError(f"El modelo {model_id} no soporta la API de conversación")
            
            parameters = parameters or {}
            
            request_body = {
                "modelId": model_id,
                "messages": messages,
                "inferenceConfig": {
                    "maxTokens": parameters.get("max_tokens", 1024),
                    "temperature": parameters.get("temperature", 0.3),
                    "topP": parameters.get("top_p", 0.2)
                }
            }
            
            if system_prompt:
                request_body["system"] = [{"text": system_prompt}]
            
            response = self.bedrock_client.converse(**request_body)
            
            content = response['output']['message']['content'][0]['text']
            usage = response['usage']
            
            return {
                "text": content,
                "input_tokens": usage['inputTokens'],
                "output_tokens": usage['outputTokens'],
                "total_tokens": usage['inputTokens'] + usage['outputTokens'],
                "stop_reason": response.get('stopReason', '')
            }
            
        except ClientError as error:
            logger.error(f"Error en converse: {error}")
            raise error
    
    def get_embedding(
        self,
        model: Union[str, BedrockModel],
        text: str,
        input_type: str = "search_document"
    ) -> List[float]:
        """
        Obtiene embeddings de texto.
        
        :param model: ID del modelo de embeddings (string o enum)
        :param text: Texto para generar embedding
        :param input_type: Tipo de input
        :return: Vector de embeddings
        """
        model_id = self._get_model_id(model)
        
        try:
            # CORREGIDO: Usa la instancia del modelo directamente
            model_instance = ModelFactory.get_model(model_id)
            
            if "embeddings" not in model_instance.supported_features:
                raise ValueError(f"El modelo {model_id} no soporta embeddings")
            
            body = {
                "inputText": text,
                "inputType": input_type
            }
            
            response = self.bedrock_client.invoke_model(
                modelId=model_id,
                body=json.dumps(body)
            )
            
            response_body = json.loads(response["body"].read())
            embedding = response_body.get("embedding", [])
            
            logger.info(f"Embedding generado para texto: {text[:50]}...")
            return embedding
            
        except ClientError as error:
            logger.error(f"Error generando embedding: {error}")
            raise error
    
    def function_call(
        self,
        model: Union[str, BedrockModel],
        messages: List[Dict[str, Any]],
        functions: List[Dict[str, Any]],
        function_call: str = "auto",
        parameters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Realiza una llamada con soporte para function calling.
        
        :param model: ID del modelo (string o enum)
        :param messages: Lista de mensajes
        :param functions: Lista de funciones disponibles
        :param function_call: Política de llamada de funciones
        :param parameters: Parámetros adicionales
        :return: Respuesta con información de función si aplica
        """
        model_id = self._get_model_id(model)
        
        try:
            # CORREGIDO: Usa la instancia del modelo directamente
            model_instance = ModelFactory.get_model(model_id)
            
            if "function_calling" not in model_instance.supported_features:
                raise ValueError(f"El modelo {model_id} no soporta function calling")
            
            if hasattr(model_instance, 'format_function_call'):
                body = model_instance.format_function_call(messages, functions, function_call)
            else:
                body = {
                    "messages": messages,
                    "functions": functions,
                    "function_call": function_call
                }
            
            if parameters:
                body.update(parameters)
            
            logger.info(f"Invocando modelo {model_id} con function calling")
            response = self.bedrock_client.invoke_model(
                modelId=model_id,
                body=json.dumps(body)
            )
            
            response_body = response["body"].read()
            response_json = json.loads(response_body)
            
            if hasattr(model_instance, 'parse_function_response'):
                return model_instance.parse_function_response(response_json)
            else:
                return model_instance.parse_response(response_json)
            
        except ClientError as error:
            logger.error(f"Error en function call: {error}")
            raise error
    
    def get_supported_features(self, model: Union[str, BedrockModel]) -> List[str]:
        """
        Obtiene las características soportadas por un modelo.
        
        :param model: ID del modelo (string o enum)
        :return: Lista de características soportadas
        """
        model_id = self._get_model_id(model)
        
        try:
            model_instance = ModelFactory.get_model(model_id)
            return model_instance.supported_features
        except Exception as error:
            logger.error(f"Error obteniendo características: {error}")
            return []
    
    def get_model_info(self, model: BedrockModel) -> Dict[str, Any]:
        """
        Obtiene información detallada sobre un modelo.
        
        :param model: Modelo enum
        :return: Información del modelo
        """
        try:
            model_instance = ModelFactory.get_model(model.value)
            
            return {
                "model_id": model.value,
                "provider": BedrockModel.get_provider(model),
                "category": BedrockModelCategory.get_model_category(model).value,
                "supported_features": model_instance.supported_features,
                "default_parameters": model_instance.default_parameters
            }
        except Exception as error:
            logger.error(f"Error obteniendo información del modelo: {error}")
            return {}