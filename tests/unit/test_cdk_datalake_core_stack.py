import aws_cdk as core
import aws_cdk.assertions as assertions

from stacks.cdk_datalake_core_stack import CdkDatalakeCoreStack

# example tests. To run these tests, uncomment this file along with the example
# resource in stacks/cdk_datalake_core_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = CdkDatalakeCoreStack(app, "cdk-datalake-core")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
