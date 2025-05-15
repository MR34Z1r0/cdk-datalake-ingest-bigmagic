import boto3
import csv
import os

region_name = 'us-east-1'
boto3.setup_default_session(profile_name='prd-valorx-admin')

dynamodb = boto3.resource('dynamodb', region_name=region_name)  
table_name = 'sofia-dev-datalake-configuration-ddb' #produccion
table = dynamodb.Table(table_name)
 
endpoints = ['PEBDDATA2'] #produccion - sellin 


def subir_csv_a_dynamo(archivo_csv):
    with open(archivo_csv, 'r') as archivo:
        reader = csv.DictReader(archivo)  # Lee el CSV como diccionario
        for fila in reader:
            if 'STATUS' in fila:
                if fila['STATUS'] != 'ACTIVE' and fila['STATUS'] != 'A' and fila['STATUS'] != 'a':
                    continue
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
                    # Inserta cada fila en la tabla
                    fila['ACTIVE_FLAG'] = "Y"
                        #if table name starts with t_ or i_ then Load Type is incremental
                    #if fila['SOURCE_TABLE_TYPE'] == 't':
                    #    if endpoint == 'SALESFORCE_ING':
                    #        fila['LOAD_TYPE'] = 'days_off'
                    #        fila['NUM_DAYS'] = '10'
                    #    else:
                    #        fila['LOAD_TYPE'] = 'incremental'
                    #else:
                    #    fila['LOAD_TYPE'] = 'full'
                    if 'LOAD_TYPE' not in fila:
                        fila['LOAD_TYPE'] = 'full'
                    if endpoint == 'SALESFORCE_ING':
                        fila['TARGET_TABLE_NAME'] = endpoint.upper() + '_' + fila['SOURCE_TABLE'].upper()
                    else:
                        fila['TARGET_TABLE_NAME'] = endpoint.upper() + '_' + fila['STAGE_TABLE_NAME'].upper()
                    fila['ENDPOINT'] = endpoint.upper().replace("_ING","")
                    fila['CRAWLER'] = False
                    #fila['CRAWLER'] = True
                    fila['DELAY_INCREMENTAL_INI'] = fila['DELAY_INCREMENTAL_INI'].replace("'",'')
                    response = table.put_item(Item=fila)
                    #print(f"Elemento subido: {fila}")
                except Exception as e:
                    print(f"Error subiendo {fila}: {e}")
            
 
subir_csv_a_dynamo('datalake_tables_bigmagic.csv') #add new tables



