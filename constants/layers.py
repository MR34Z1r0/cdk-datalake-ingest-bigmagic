import os
from aje_cdk_libs.constants.project_config import ProjectConfig

class Layers:
    """Centralized path configurations for local and AWS assets"""
    def __init__(self, app_config: dict, region: str, account: str):
        aws_lambda_layers = app_config.get("artifacts").get("aws_lambda_layers")
        for key in aws_lambda_layers:
            aws_lambda_layers[key] = aws_lambda_layers[key].replace("${region}", region).replace("${account}", account)
        self.AWS_LAMBDA_LAYERS = aws_lambda_layers