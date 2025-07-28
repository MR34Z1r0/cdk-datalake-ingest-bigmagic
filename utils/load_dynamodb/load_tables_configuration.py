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

enterprise = 'sofia'
project_name = 'datalake'
region_name = args.REGION
team = args.TEAM.lower()  # Team name
datasource = args.DATASOURCE  # Data source
endpoints = args.ENDPOINTS.split(',') #produccion
environment = args.ENVIRONMENT.lower()  # Environment name

boto3.setup_default_session(profile_name='prd-valorx-admin')

dynamodb = boto3.resource('dynamodb', region_name=region_name)  
table_name = f'{enterprise}-{environment}-{project_name}-configuration-ddb' #produccion
table = dynamodb.Table(table_name) 
  

def subir_csv_a_dynamo(archivo_csv):
    # Intentar diferentes codificaciones
    encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
    
    for encoding in encodings:
        try:
            with open(archivo_csv, 'r', encoding=encoding) as archivo:
                reader = csv.DictReader(archivo, delimiter=';')
                rows = list(reader)
                break
        except UnicodeDecodeError:
            continue
    else:
        print("No se pudo leer el archivo con ninguna codificación")
        return
    
    for fila in rows:
        # Limpiar BOM y caracteres especiales de las claves
        fila_limpia = {}
        for key, value in fila.items():
            clean_key = key.replace('\ufeff', '').replace('ï»¿', '').strip()
            fila_limpia[clean_key] = value
        
        fila = fila_limpia
        
        if 'STATUS' in fila:
            if fila['STATUS'] != 'ACTIVE' and fila['STATUS'] != 'A' and fila['STATUS'] != 'a':
                continue
                
        for endpoint in endpoints:
            try:
                # All columns if file at this point, if have, remove " at begin and end
                for key in fila.keys():
                    # Only if column is string
                    if isinstance(fila[key], str):
                        if fila[key].startswith('"'):
                            fila[key] = fila[key][1:]
                        if fila[key].endswith('"'):
                            fila[key] = fila[key][0:-1]
                
                # Inserta cada fila en la tabla
                fila['ACTIVE_FLAG'] = "Y"
                
                if 'LOAD_TYPE' not in fila:
                    fila['LOAD_TYPE'] = 'full'
                    
                if endpoint == 'SALESFORCE_ING':
                    fila['TARGET_TABLE_NAME'] = endpoint.upper() + '_' + fila['SOURCE_TABLE'].upper()
                else:
                    fila['TARGET_TABLE_NAME'] = endpoint.upper() + '_' + fila['STAGE_TABLE_NAME'].upper()
                    
                fila['TEAM'] = team
                fila['DATA_SOURCE'] = datasource
                fila['ENDPOINT_NAME'] = endpoint #endpoint.upper().replace("_ING","")
                fila['INSTANCE'] = args.INSTANCE.upper()  # Add country from argument
                fila['CRAWLER'] = False
                
                # Verificar si existe la clave antes de procesarla
                if 'DELAY_INCREMENTAL_INI' in fila and fila['DELAY_INCREMENTAL_INI']:
                    fila['DELAY_INCREMENTAL_INI'] = fila['DELAY_INCREMENTAL_INI'].replace("'",'')
                
                response = table.put_item(Item=fila)
                #print(f"Elemento subido: {fila}")
                
            except Exception as e:
                print(f"Error subiendo {fila}: {e}")
             
subir_csv_a_dynamo(f'{project_name}_tables_{team}_{datasource}.csv') #add new tables



