import boto3
import csv
import os

# Configure DynamoDB client
region_name = 'us-east-1'
boto3.setup_default_session(profile_name='prd-valorx-admin', region_name=region_name)
dynamodb = boto3.resource('dynamodb')

# Table information
table_name = 'sofia-dev-datalake-columns-specifications-ddb'  # Production table
table = dynamodb.Table(table_name)

# Endpoints to export
endpoints = ['PEBDDATA']  # Production endpoint

def convertir_desde_booleano(valor):
    if valor is True:
        return 'T'
    elif valor is False:
        return 'F'
    else:
        return 'F'

def descargar_dynamo_a_csv(archivo_csv):
    items = []
    
    # For each endpoint, scan the table and filter by TARGET_TABLE_NAME
    for endpoint in endpoints:
        try:
            # Build filter expression to match the endpoint pattern
            filter_expression = boto3.dynamodb.conditions.Key('TARGET_TABLE_NAME').begins_with(
                endpoint.upper() + '_'
            )
            
            # Scan the table with the filter
            response = table.scan(
                FilterExpression=filter_expression
            )
            items.extend(response['Items'])
            
            # Handle pagination if there are more items
            while 'LastEvaluatedKey' in response:
                response = table.scan(
                    FilterExpression=filter_expression,
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response['Items'])
                
            print(f"Found {len(items)} items for endpoint {endpoint}")
            
        except Exception as e:
            print(f"Error retrieving data for endpoint {endpoint}: {e}")
    
    # If items were found, write to CSV
    if items:
        # Get all possible field names (columns) from all items
        fieldnames = set()
        for item in items:
            fieldnames.update(item.keys())
        
        # Make sure the required columns are first in the CSV
        required_columns = ['COLUMN_NAME', 'COLUMN_ID', 'IS_FILTER_DATE', 'IS_ID', 
                           'IS_ORDER_BY', 'IS_PARTITION', 'NEW_DATA_TYPE', 
                           'TABLE_NAME', 'TRANSFORMATION']
        
        # Remove TARGET_TABLE_NAME as we don't need it in the CSV
        if 'TARGET_TABLE_NAME' in fieldnames:
            fieldnames.remove('TARGET_TABLE_NAME')
        
        # Sort fieldnames to ensure required columns come first
        fieldnames = required_columns + [f for f in fieldnames if f not in required_columns]
        
        with open(archivo_csv, 'w', newline='') as archivo:
            # Aquí está el cambio: especificar delimiter='|'
            writer = csv.DictWriter(archivo, fieldnames=fieldnames, delimiter='|')
            writer.writeheader()
            
            for item in items:
                # Remove TARGET_TABLE_NAME field as it's constructed from TABLE_NAME and endpoint
                if 'TARGET_TABLE_NAME' in item:
                    del item['TARGET_TABLE_NAME']
                
                # Convert boolean values back to 'T'/'F' format
                for bool_field in ['IS_FILTER_DATE', 'IS_ID', 'IS_ORDER_BY', 'IS_PARTITION']:
                    if bool_field in item:
                        item[bool_field] = convertir_desde_booleano(item[bool_field])
                
                writer.writerow(item)
        
        print(f"Datos exportados exitosamente a {archivo_csv}")
    else:
        print("No se encontraron elementos para exportar.")

# Execute the function
descargar_dynamo_a_csv('datalake_columns_bigmagic.csv')