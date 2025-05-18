import boto3
import csv
import os

# Configure DynamoDB client
region_name = 'us-east-1'
boto3.setup_default_session(profile_name='prd-valorx-admin', region_name=region_name)
dynamodb = boto3.resource('dynamodb')

# Table information
table_name = 'sofia-dev-datalake-configuration-ddb'  # Production table
table = dynamodb.Table(table_name)

# Endpoints to export
endpoints = ['PEBDDATA']  # Production endpoint - sellin

def descargar_dynamo_a_csv(archivo_csv):
    items = []
    
    # For each endpoint, scan the table and filter by ENDPOINT
    for endpoint in endpoints:
        try:
            endpoint_filter = endpoint.upper().replace("_ING", "")
            
            # Build filter expression to match the endpoint
            filter_expression = boto3.dynamodb.conditions.Attr('ENDPOINT').eq(endpoint_filter)
            
            # Add ACTIVE_FLAG filter to get only active records
            active_filter = boto3.dynamodb.conditions.Attr('ACTIVE_FLAG').eq('Y')
            combined_filter = filter_expression & active_filter
            
            # Scan the table with the filter
            response = table.scan(
                FilterExpression=combined_filter
            )
            items.extend(response['Items'])
            
            # Handle pagination if there are more items
            while 'LastEvaluatedKey' in response:
                response = table.scan(
                    FilterExpression=combined_filter,
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response['Items'])
                
            print(f"Found {len(items)} items for endpoint {endpoint}")
            
        except Exception as e:
            print(f"Error retrieving data for endpoint {endpoint}: {e}")
    
    # If items were found, write to CSV
    if items:
        # Required columns as specified
        required_columns = [
            'COLUMNS', 'DELAY_INCREMENTAL_INI', 'FILTER_COLUMN', 'FILTER_DATA_TYPE',
            'FILTER_EXP', 'ID_COLUMN', 'JOIN_EXPR', 'PROCESS_ID', 'SOURCE_SCHEMA',
            'SOURCE_TABLE', 'SOURCE_TABLE_TYPE', 'STAGE_TABLE_NAME'
        ]
        
        with open(archivo_csv, 'w', newline='') as archivo:
            # Modificación aquí: agregar delimiter='|'
            writer = csv.DictWriter(archivo, fieldnames=required_columns, delimiter='|')
            writer.writeheader()
            
            for item in items:
                row = {}
                # Only include the required columns
                for column in required_columns:
                    if column in item:
                        row[column] = item[column]
                    else:
                        row[column] = ''  # Add empty string for missing columns
                
                writer.writerow(row)
        
        print(f"Datos exportados exitosamente a {archivo_csv}")
    else:
        print("No se encontraron elementos para exportar.")

# Execute the function
descargar_dynamo_a_csv('datalake_tables_bigmagic.csv')