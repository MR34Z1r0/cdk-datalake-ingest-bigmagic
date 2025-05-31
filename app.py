#!/usr/bin/env python3
import os
import aws_cdk as cdk
from stacks.cdk_datalake_ingest_apdayc_stack import CdkDatalakeIngestApdaycStack
from aje_cdk_libs.constants.environments import Environments
from aje_cdk_libs.constants.project_config import ProjectConfig
from dotenv import load_dotenv

load_dotenv() 
app = cdk.App()

CONFIG = app.node.try_get_context("project_config")
CONFIG["account_id"] = os.getenv("ACCOUNT_ID", None)
CONFIG["region_name"] = os.getenv("REGION_NAME", None)
CONFIG["environment"] = os.getenv("ENVIRONMENT", None) 
CONFIG["separator"] = os.getenv("SEPARATOR", "-") 
project_config = ProjectConfig.from_dict(CONFIG)
      
CdkDatalakeIngestApdaycStack(
    app, 
    "CdkDatalakeIngestApdaycStack",
    project_config,
    env=cdk.Environment(
        account=project_config.account_id,
        region=project_config.region_name
    )
)

app.synth()