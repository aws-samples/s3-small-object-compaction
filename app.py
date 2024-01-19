#!/usr/bin/env python3
import aws_cdk as cdk
from compaction.compaction_stack import CompactionStack
from cdk_nag import ( AwsSolutionsChecks, NagSuppressions )

app = cdk.App()
cdk.Aspects.of(app).add(AwsSolutionsChecks())
stack = CompactionStack(app, "S3ObjectCompactionStack")

NagSuppressions.add_stack_suppressions(stack, [{"id":"AwsSolutions-IAM4", "reason":"AWSLambdaBasicExecutionRole used for Lambda function logging"}])
NagSuppressions.add_stack_suppressions(stack, [{"id":"AwsSolutions-IAM5", "reason":"Source and Dest S3 Buckets are not created by this stack, so the resource policies are appended to rather than defined explicitly"}])

app.synth()
