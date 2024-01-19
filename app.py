#!/usr/bin/env python3
import aws_cdk as cdk
from compaction.compaction_stack import CompactionStack


app = cdk.App()
CompactionStack(app, "S3ObjectCompactionStack")

app.synth()
