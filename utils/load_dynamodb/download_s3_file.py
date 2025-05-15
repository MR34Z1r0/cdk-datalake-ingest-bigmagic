# Script to read from an S3 bucket and path a download into a local folder
# download into google drive location

import boto3
import os
from dotenv import load_dotenv
from time import sleep
import random

load_dotenv()

session = boto3.Session(
    aws_access_key_id=os.getenv('aws_access_key_id'),
    aws_secret_access_key=os.getenv('aws_secret_access_key'),
    aws_session_token=os.getenv('aws_session_token'),
    region_name='us-east-2'
)

s3 = session.client('s3')

# Path to download Raw files
#bucket_name = 'datalake-ingestion-datalakeingestionajeprdrawbucke-geoukmo6lblu'
#s3_path = 'athenea'

# Path to artifacts analytics Prod files
bucket_name = 'datalake-ingestion-aje-prd-artifacts-bucket'
s3_path = 'athenea/artifacts'

# Path to download files
local_path = 's3_downloads'
log_file_path = 'download_log.txt'

# download files inside the s3_path and keep the same structure in the local_path
# example: s3://datalake-ingestion-ajedevstage/example/path/group1/file1.csv -> s3_downloads/group1/file1.csv
# Only download files if contains following string in the name: 'year=' and 'month=' and 'day='
def download_file(bucket_name, s3_path, local_path):
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_path)
    a = 1
    with open(log_file_path, 'a') as log_file:
        for page in pages:
            for content in page['Contents']:
                a += 1
                file_name = content['Key'].split('/')[-1]
                if file_name == '':
                    continue
                s3_sub_path_pre = content['Key'].replace(file_name, '')
                # Handle special characters in the beginning of the file name: ' ', '-'
                if file_name[0] == ' ':
                    file_name = file_name[1:]
                if file_name[0] == '-':
                    file_name = file_name[1:]
                file_name = file_name.replace(':', '_')
                # sub_path use file_name to create the folder structure
                s3_sub_path_pre = s3_sub_path_pre.replace(s3_path, '') + '/' + file_name
                s3_sub_path = s3_sub_path_pre.replace('//', '/')
                local_sub_path = s3_sub_path.replace(file_name, '')
                #randome name: adih08nduan0fua3.csv.gz
                #random_name = str(a) + '.csv.gz'
                #s3_sub_path = s3_sub_path.replace(file_name, random_name)

                #validation for raw files
                #if 'year=' in s3_sub_path and 'month=' in s3_sub_path and 'day=' in s3_sub_path and ('year=2025' in s3_sub_path and ('month=02' in s3_sub_path) and ('day=01' in s3_sub_path)):
                
                #validation for artifacts files
                if True:
                    # If file already exists, skip download
                    if os.path.exists(local_path + s3_sub_path):
                        log_file.write(f"File already exists: {content['Key']} in {local_path + s3_sub_path}\n")
                        print(f"File already exists: {content['Key']} in {local_path + s3_sub_path}")
                    else:
                        if not os.path.exists(local_path + local_sub_path):
                            os.makedirs(local_path + local_sub_path)
                        s3.download_file(bucket_name, content['Key'], local_path + s3_sub_path)
                        log_file.write(f"File downloaded: {content['Key']} into {local_path + s3_sub_path}\n")
                        print(f"File downloaded: {content['Key']} into {local_path + s3_sub_path}")


download_file(bucket_name, s3_path, local_path)
