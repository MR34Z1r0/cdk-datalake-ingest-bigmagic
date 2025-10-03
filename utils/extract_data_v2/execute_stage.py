import boto3
import json
import argparse
from datetime import datetime
from dotenv import load_dotenv
import os

# Cargar variables de entorno desde .env
load_dotenv()

def execute_step_function(process_id, instance, endpoint_name):
    """
    Ejecuta un Step Function de AWS con los parámetros especificados.
    
    Args:
        process_id: ID del proceso
        instance: Instancia (ej: PE)
        endpoint_name: Nombre del endpoint (ej: PEBDDATA2)
    """
    
    # Obtener configuración desde variables de entorno
    state_machine_arn = os.getenv('STEP_FUNCTION_ARN')
    aws_region = os.getenv('REGION', 'us-east-1')
    aws_profile = os.getenv('AWS_PROFILE')
    
    if not state_machine_arn:
        raise ValueError("❌ Error: STEP_FUNCTION_ARN no está configurado en el archivo .env")
    
    if not aws_profile:
        raise ValueError("❌ Error: AWS_PROFILE no está configurado en el archivo .env")
    
    # Inicializar sesión de boto3 con el profile especificado
    session = boto3.Session(
        profile_name=aws_profile,
        region_name=aws_region
    )
    
    # Crear cliente de Step Functions usando la sesión
    client = session.client('stepfunctions')
    
    # Preparar los datos de entrada
    input_data = {
        "process_id": process_id,
        "endpoint_name": endpoint_name,
        "instance": instance,
        "scheduled_execution": False,
        "run_extract": False
    }
    
    # Nombre único para la ejecución
    execution_name = f"execution_{process_id}_{instance}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        print(f"🔧 Usando AWS Profile: {aws_profile}")
        print(f"🌍 Región: {aws_region}")
        print(f"⚙️  Step Function ARN: {state_machine_arn}\n")
        
        # Ejecutar el Step Function
        response = client.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps(input_data)
        )
        
        print(f"✅ Step Function ejecutado exitosamente!")
        print(f"Execution ARN: {response['executionArn']}")
        print(f"Start Date: {response['startDate']}")
        print(f"\nDatos enviados:")
        print(json.dumps(input_data, indent=2))
        
        return response
        
    except Exception as e:
        print(f"❌ Error al ejecutar el Step Function: {str(e)}")
        raise

def main():
    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(description='Ejecutar Step Function de AWS')
    parser.add_argument('--process-id', required=True, help='ID del proceso')
    parser.add_argument('--instance', required=True, help='Instancia (ej: PE)')
    parser.add_argument('--endpoint', required=True, help='Nombre del endpoint (ej: PEBDDATA2)')
    
    args = parser.parse_args()
    
    # Ejecutar el Step Function
    execute_step_function(
        process_id=args.process_id,
        instance=args.instance,
        endpoint_name=args.endpoint
    )

if __name__ == "__main__":
    main()