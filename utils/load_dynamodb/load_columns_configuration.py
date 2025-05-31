import boto3
import csv
import os
import argparse
#from dotenv import load_dotenv

#load_dotenv()
#

# Configura tu cliente de DynamoDB
#session = boto3.Session(
#    aws_access_key_id=os.getenv('aws_access_key_id'),
#    aws_secret_access_key=os.getenv('aws_secret_access_key'),
#    aws_session_token=os.getenv('aws_session_token'),
#    region_name='us-east-2'
#)
region_name = 'us-east-1'
boto3.setup_default_session(profile_name='prd-valorx-admin')
dynamodb = boto3.resource('dynamodb', region_name=region_name)  # Cambia a tu región
#table_name = 'datalakeIngestion-ajedevstagecolumnsspecifications21A57026-MIFJIFSNZ8CY' #desarrollo
table_name = 'sofia-dev-datalake-columns-specifications-ddb' #produccion
table = dynamodb.Table(table_name)

parser = argparse.ArgumentParser(description='Extract data from source and load to S3')
parser.add_argument("-t", '--TEAM', required=True, help='Team name')
parser.add_argument("-d", '--DATASOURCE', required=True, help='Data source name')
parser.add_argument("-e", '--ENDPOINTS', help='List of endpoints to process, separate for ","')
parser.add_argument("-i", '--INSTANCE', help='Instance name', default='PE')  # Default to 'PE'
args = parser.parse_args()

team = args.TEAM  # Team name
datasource = args.DATASOURCE  # Data source
endpoints = args.ENDPOINTS.split(',') #produccion

def convertir_a_booleano(valor):
    if valor == 'T':
        return True
    elif valor == 't':
        return True
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
    with open(archivo_csv, 'r') as archivo:
        reader = csv.DictReader(archivo, delimiter=';')  # Lee el CSV como diccionario
        for fila in reader:
            for endpoint in endpoints:
                try:
                    #All columns if file at this point, if have, remove " at begin and end
                    for key in fila.keys():
                        #Only if column is string
                        if isinstance(fila[key], str):
                            if fila[key].startswith('"'):
                                fila[key] = fila[key][1:]
                            if fila[key].endswith('"'):
                                fila[key] = fila[key][0:-1]
                    if isinstance(fila['COLUMN_ID'], str):
                        fila['COLUMN_ID'] = fila['COLUMN_ID'].replace("'",'')
                    # Inserta cada fila en la tabla
                    fila['TARGET_TABLE_NAME'] = endpoint.upper() + '_' + fila['TABLE_NAME'].upper()
                    fila['IS_ID'] = convertir_a_booleano(fila['IS_ID'])
                    fila['IS_ORDER_BY'] = convertir_a_booleano(fila['IS_ORDER_BY'])
                    fila['IS_PARTITION'] = convertir_a_booleano(fila['IS_PARTITION'])
                    fila['IS_FILTER_DATE'] = convertir_a_booleano(fila['IS_FILTER_DATE'])
                    fila['COLUMN_ID'] = int(fila['COLUMN_ID'])
                    fila['TEAM'] = team
                    fila['DATA_SOURCE'] = datasource
                    fila['ENDPOINT_NAME'] = endpoint
                    fila['INSTANCE'] = args.INSTANCE.upper()  # Add country from argument

                    response = table.put_item(Item=fila)
                    #print(f"Elemento subido: {fila}")
                except Exception as e:
                    print(f"Error subiendo {fila}: {e}")

subir_csv_a_dynamo(f'datalake_columns_{team}_{datasource}.csv') #add new columns




