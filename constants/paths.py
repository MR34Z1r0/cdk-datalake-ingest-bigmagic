import os
from aje_cdk_libs.constants.project_config import ProjectConfig

class Paths:
    '''Centralized path configurations for local and AWS assets'''
    def __init__(self, app_config: dict):
        self.LOCAL_ARTIFACTS = app_config.get('artifacts').get('local')
        self.TEAM = app_config.get('team')
        self.DATASOURCE = app_config.get('datasource')        
        # Local paths 
        self.LOCAL_ARTIFACTS_CONFIGURE = f'{self.LOCAL_ARTIFACTS}/configuration' 
        self.LOCAL_ARTIFACTS_CONFIGURE_CSV = f'{self.LOCAL_ARTIFACTS_CONFIGURE}/csv' 
        
        self.LOCAL_ARTIFACTS_LAMBDA = f'{self.LOCAL_ARTIFACTS}/aws-lambda' 
        self.LOCAL_ARTIFACTS_LAMBDA_CODE = f'{self.LOCAL_ARTIFACTS_LAMBDA}/code' 
        self.LOCAL_ARTIFACTS_LAMBDA_CODE_RAW = f'{self.LOCAL_ARTIFACTS_LAMBDA_CODE}/raw' 
        self.LOCAL_ARTIFACTS_LAMBDA_CODE_STAGE = f'{self.LOCAL_ARTIFACTS_LAMBDA_CODE}/stage' 
        self.LOCAL_ARTIFACTS_LAMBDA_LAYER = f'{self.LOCAL_ARTIFACTS_LAMBDA}/layer' 
        self.LOCAL_ARTIFACTS_LAMBDA_DOCKER = f'{self.LOCAL_ARTIFACTS_LAMBDA}/docker' 
        
        self.LOCAL_ARTIFACTS_GLUE = f'{self.LOCAL_ARTIFACTS}/aws-glue' 
        self.LOCAL_ARTIFACTS_GLUE_CODE = f'{self.LOCAL_ARTIFACTS_GLUE}/code' 
        self.LOCAL_ARTIFACTS_GLUE_CODE_RAW = f'{self.LOCAL_ARTIFACTS_GLUE_CODE}/raw' 
        self.LOCAL_ARTIFACTS_GLUE_CODE_STAGE = f'{self.LOCAL_ARTIFACTS_GLUE_CODE}/stage' 
        self.LOCAL_ARTIFACTS_GLUE_JARS = f'{self.LOCAL_ARTIFACTS_GLUE}/jars' 
        self.LOCAL_ARTIFACTS_GLUE_LIBS = f'{self.LOCAL_ARTIFACTS_GLUE}/libs'
        self.LOCAL_ARTIFACTS_GLUE_LAYER = f'{self.LOCAL_ARTIFACTS_GLUE}/layer'
          
        # AWS paths     
        self.AWS_ARTIFACTS_CONFIGURE = f'{self.TEAM}/{self.DATASOURCE}/configuration'
        self.AWS_ARTIFACTS_CONFIGURE_CSV = f'{self.AWS_ARTIFACTS_CONFIGURE}/csv'
        self.AWS_ARTIFACTS_GLUE = f'{self.TEAM}/{self.DATASOURCE}/aws-glue'
        self.AWS_ARTIFACTS_GLUE_CSV = f'{self.AWS_ARTIFACTS_GLUE}/csv'
        self.AWS_ARTIFACTS_GLUE_CODE = f'{self.AWS_ARTIFACTS_GLUE}/code'
        self.AWS_ARTIFACTS_GLUE_CODE_RAW = f'{self.AWS_ARTIFACTS_GLUE_CODE}/raw'
        self.AWS_ARTIFACTS_GLUE_CODE_STAGE = f'{self.AWS_ARTIFACTS_GLUE_CODE}/stage'
        self.AWS_ARTIFACTS_GLUE_JARS = f'{self.AWS_ARTIFACTS_GLUE}/jars'
        self.AWS_ARTIFACTS_GLUE_LIBS = f'{self.AWS_ARTIFACTS_GLUE}/libs'
        self.AWS_ARTIFACTS_GLUE_LAYER = f'{self.AWS_ARTIFACTS_GLUE}/layer'