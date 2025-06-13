import boto3
import csv
import os
import argparse

parser = argparse.ArgumentParser(description='Extract data from source and load to S3')
parser.add_argument("-r", '--REGION', required=True, help='Region name', default ='us-east-1')  # Default to 'us-east-1'
parser.add_argument("-n", '--ENVIRONMENT', required=True, help='Environment name', default='dev')  # Default to 'dev'
parser.add_argument("-t", '--TEAM', required=True, help='Team name')
parser.add_argument("-d", '--DATASOURCE', required=True, help='Data source name')
parser.add_argument("-e", '--ENDPOINTS', help='List of endpoints to process, separate for ","')
parser.add_argument("-i", '--INSTANCE', help='Instance name', default='PE')  # Default to 'PE'
args = parser.parse_args()

project_name = 'datalake'
region_name = args.REGION
team = args.TEAM.lower()  # Team name
datasource = args.DATASOURCE  # Data source
endpoints = args.ENDPOINTS.split(',') #produccion
environment = args.ENVIRONMENT.lower()  # Environment name

boto3.setup_default_session(profile_name='prd-valorx-admin')

dynamodb = boto3.resource('dynamodb', region_name=region_name)  
table_name = f'{team}-{environment}-{project_name}-configuration-ddb' #produccion
table = dynamodb.Table(table_name)  

def descargar_dynamo_a_csv(archivo_csv):
    items = []
    
    # For each endpoint, scan the table and filter by ENDPOINT
    for endpoint in endpoints:
        try:            
            # Build filter expression to match the endpoint
            filter_expression = boto3.dynamodb.conditions.Attr('ENDPOINT_NAME').eq(endpoint)
            # Scan the table with the filter
            response = table.scan(
                FilterExpression=filter_expression
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
        # Get all possible field names (columns) from all items
        fieldnames = set()
        for item in items:
            fieldnames.update(item.keys())

        # Required columns as specified
        required_columns = [
            'COLUMNS', 'DELAY_INCREMENTAL_INI', 'FILTER_COLUMN', 'PARTITION_COLUMN', 'FILTER_DATA_TYPE',
            'FILTER_EXP', 'ID_COLUMN', 'JOIN_EXPR', 'PROCESS_ID', 'SOURCE_SCHEMA',
            'SOURCE_TABLE', 'SOURCE_TABLE_TYPE', 'STAGE_TABLE_NAME', 'ENDPOINT_NAME', 'DATA_SOURCE', 'TEAM'
        ]
        
        with open(archivo_csv, 'w', newline='') as archivo:
            # Modificación aquí: agregar delimiter='|'
            writer = csv.DictWriter(archivo, fieldnames=required_columns, delimiter=';')
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
descargar_dynamo_a_csv(f'{project_name}_tables_{team}_{datasource}.csv')