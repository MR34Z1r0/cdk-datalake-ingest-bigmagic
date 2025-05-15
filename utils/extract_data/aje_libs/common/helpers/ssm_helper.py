# Built-in imports
from typing import Optional

# External imports
import boto3
from botocore.exceptions import ClientError

# Own imports
from ..logger import custom_logger

logger = custom_logger(__name__)

class SSMParameterHelper:
    """Ayudante personalizado para Systems Manager Parameter Store que simplifica la obtención de parámetros."""

    def __init__(self, parameter_name: str) -> None:
        """
        :param parameter_name (str): Nombre del parámetro a recuperar.
        """
        self.parameter_name = parameter_name
        self.client_ssm = boto3.client("ssm")
        logger.info(f"Inicializando SSMParameterHelper para el parámetro: {parameter_name}")

    def get_parameter_value(self, with_decryption: bool = True) -> Optional[str]:
        """
        Obtiene el valor del parámetro desde AWS SSM Parameter Store.

        :param with_decryption (bool): Si se debe desencriptar el valor (para parámetros tipo SecureString).
        :return: Valor del parámetro como string.
        """
        try:
            logger.info(f"Intentando obtener el parámetro: {self.parameter_name}")
            
            response = self.client_ssm.get_parameter(
                Name=self.parameter_name,
                WithDecryption=with_decryption
            )
            
            value = response["Parameter"]["Value"]
            logger.info(f"Parámetro obtenido con éxito: {self.parameter_name}")
            return value

        except ClientError as e:
            logger.error(f"Error al obtener el parámetro de AWS SSM: {self.parameter_name}")
            logger.error(f"Código de error: {e.response['Error']['Code']}")
            logger.error(f"Mensaje de error: {e.response['Error']['Message']}")
            raise e
        except KeyError:
            logger.error("No se encontró el campo 'Parameter' en la respuesta")
            raise KeyError("El campo 'Parameter' no está presente en la respuesta de SSM")

