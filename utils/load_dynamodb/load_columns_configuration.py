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
table_name = f'{enterprise}-{environment}-{project_name}-columns-specifications-ddb' #produccion
table = dynamodb.Table(table_name) 

def convertir_a_booleano(valor):
    if valor == 'T':
        return True
    elif valor == 't':
        return True
    if valor == 'True':
        return True
    elif valor == 'False':
        return False
    elif valor ==  True:
        return True
    elif valor == False:
        return False
    elif valor == 'f':
        return False
    elif valor == 'F':
        return False
    else:
        return False

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
        for endpoint in endpoints:
            try:
                # Limpiar BOM y caracteres especiales de las claves
                fila_limpia = {}
                for key, value in fila.items():
                    clean_key = key.replace('\ufeff', '').replace('ï»¿', '').strip()
                    fila_limpia[clean_key] = value
                
                fila = fila_limpia
                
                # Resto del código igual...
                for key in fila.keys():
                    if isinstance(fila[key], str):
                        if fila[key].startswith('"'):
                            fila[key] = fila[key][1:]
                        if fila[key].endswith('"'):
                            fila[key] = fila[key][0:-1]
                
                if isinstance(fila['COLUMN_ID'], str):
                    fila['COLUMN_ID'] = fila['COLUMN_ID'].replace("'",'')
                
                fila['TARGET_TABLE_NAME'] = endpoint.upper() + '_' + fila['TABLE_NAME'].upper()
                fila['IS_ID'] = convertir_a_booleano(fila['IS_ID'])
                fila['IS_ORDER_BY'] = convertir_a_booleano(fila['IS_ORDER_BY'])
                fila['IS_PARTITION'] = convertir_a_booleano(fila['IS_PARTITION'])
                fila['IS_FILTER_DATE'] = convertir_a_booleano(fila['IS_FILTER_DATE'])
                fila['COLUMN_ID'] = int(fila['COLUMN_ID'])
                fila['TEAM'] = team
                fila['DATA_SOURCE'] = datasource
                fila['ENDPOINT_NAME'] = endpoint
                fila['INSTANCE'] = args.INSTANCE.upper()

                response = table.put_item(Item=fila)
                
            except Exception as e:
                print(f"Error subiendo {fila}: {e}")

subir_csv_a_dynamo(f'{project_name}_columns_{team}_{datasource}.csv') #add new columns




