#!/usr/bin/env python3
import aws_cdk as cdk
from rearc_pipeline_stack import RearcDataPipelineStack

app = cdk.App()
RearcDataPipelineStack(app, "RearcDataPipelineStack")
app.synth()
