import boto3
from botocore.exceptions import ClientError


# Configure DynamoDB client
region_name = 'us-east-1'
boto3.setup_default_session(profile_name='prd-valorx-admin', region_name=region_name)

def delete_crawler_attribute_from_dynamodb():
    # Inicializar el recurso de DynamoDB
    dynamodb = boto3.resource('dynamodb')
    
    # Referencia a la tabla
    table_name = "sofia-dev-datalake-configuration-ddb"
    table = dynamodb.Table(table_name)
    
    try:
        # Primero, escanea la tabla para encontrar elementos donde CRAWLER=true
        response = table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('CRAWLER').eq(True)
        )
        
        items = response['Items']
        
        # Para cada elemento encontrado, elimina el atributo CRAWLER
        for item in items:
            # Asumiendo que la clave primaria es 'TARGET_TABLE_NAME' basado en tu código
            primary_key_value = item['TARGET_TABLE_NAME']
            
            # Eliminar el atributo CRAWLER
            table.update_item(
                Key={
                    'TARGET_TABLE_NAME': primary_key_value
                },
                UpdateExpression="REMOVE CRAWLER"
            )
            print(f"Campo CRAWLER eliminado del elemento con TARGET_TABLE_NAME: {primary_key_value}")
            
        # Manejar paginación si hay más elementos
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('CRAWLER').eq(True),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            
            items = response['Items']
            
            for item in items:
                primary_key_value = item['TARGET_TABLE_NAME']
                
                table.update_item(
                    Key={
                        'TARGET_TABLE_NAME': primary_key_value
                    },
                    UpdateExpression="REMOVE CRAWLER"
                )
                print(f"Campo CRAWLER eliminado del elemento con TARGET_TABLE_NAME: {primary_key_value}")
                
        return "Operación completada con éxito"
    
    except ClientError as e:
        print(f"Error al eliminar el atributo CRAWLER: {e}")
        return str(e)

# Ejecutar la función
if __name__ == "__main__":
    result = delete_crawler_attribute_from_dynamodb()
    print(result)