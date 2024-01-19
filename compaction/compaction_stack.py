from aws_cdk import (
    Duration,
    Stack,
    Size,
    aws_lambda as awslambda,
    aws_s3 as s3,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_stepfunctions as stepfunctions,
    aws_logs as logs
)
from constructs import Construct
import json

class CompactionStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        #Get context variables
        source_s3_bucket_uri = self.node.try_get_context("source_s3_uri")
        target_s3_bucket_uri = self.node.try_get_context("target_s3_uri")
        previous_days = self.node.try_get_context("previous_days")
        date_format = self.node.try_get_context("date_format")

        source_s3_bucket = s3.Bucket.from_bucket_name(self, "sourceS3Bucket", source_s3_bucket_uri.split('/')[2])
        target_s3_bucket = s3.Bucket.from_bucket_name(self, "targetS3Bucket", target_s3_bucket_uri.split('/')[2])
        

        compactionFunction = awslambda.Function(self, "standaloneCompactFunction",
                                                runtime=awslambda.Runtime.PYTHON_3_12,
                                                handler="index.handler",
                                                timeout=Duration.minutes(15),
                                                ephemeral_storage_size=Size.mebibytes(2048),
                                                memory_size=1024,
                                                tracing=awslambda.Tracing.ACTIVE,
                                                code=awslambda.Code.from_asset("lambda/standalone_function_compact"))

        compactionFunction.grant_invoke(iam.ServicePrincipal("events.amazonaws.com"))

        compactionFunction.add_to_role_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:GetObject",
                "s3:ListBucket"
            ],
            resources=[
                source_s3_bucket.bucket_arn,
                source_s3_bucket.bucket_arn + "/*",
            ]))
        source_s3_bucket.grant_read(compactionFunction)

        compactionFunction.add_to_role_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:PutObject",
            ],
            resources=[
                target_s3_bucket.bucket_arn,
                target_s3_bucket.bucket_arn + "/*",
            ]))
        target_s3_bucket.grant_write(compactionFunction)

        # Uncomment to add EventBridge rule for standalone Lambda schedule
        lambdaTriggerRule = events.Rule(self, "compactionRuleStandaloneLambda",
                           enabled=False,
                           schedule=events.Schedule.rate(
                               Duration.days(int(previous_days)))
                               )

        lambdaTriggerRule.add_target(targets.LambdaFunction(
            compactionFunction,
            event=events.RuleTargetInput.from_object(
                {
                    "s3_source_uri": source_s3_bucket_uri,
                    "s3_destination_uri": target_s3_bucket_uri,
                    "date_format": date_format,
                    "duration": int(previous_days)
                }
            )
        ))

        ###############################
        ## Step Functions Components ##
        ###############################

        sfListPrefixFunction = awslambda.Function(self, "distributedMapListFunction",
                                                runtime=awslambda.Runtime.PYTHON_3_12,
                                                handler="index.lambda_handler",
                                                timeout=Duration.minutes(1),
                                                memory_size=128,
                                                code=awslambda.Code.from_asset("lambda/distributed_map_list"))
        sfListPrefixFunction.add_to_role_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:GetObject",
                "s3:ListBucket"
            ],
            resources=[
                source_s3_bucket.bucket_arn,
                source_s3_bucket.bucket_arn + "/*",
            ]))
        source_s3_bucket.grant_read(sfListPrefixFunction)


        sfCompactFunction = awslambda.Function(self, "distributedMapCompactFunction",
                                                runtime=awslambda.Runtime.PYTHON_3_12,
                                                handler="index.lambda_handler",
                                                timeout=Duration.minutes(5),
                                                ephemeral_storage_size=Size.mebibytes(2048),
                                                memory_size=128,
                                                code=awslambda.Code.from_asset("lambda/distributed_map_compact"))
        
        sfCompactFunction.add_to_role_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:GetObject",
                "s3:ListBucket"
            ],
            resources=[
                source_s3_bucket.bucket_arn,
                source_s3_bucket.bucket_arn + "/*",
            ]))
        source_s3_bucket.grant_read(sfCompactFunction)

        sfCompactFunction.add_to_role_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:PutObject",
            ],
            resources=[
                target_s3_bucket.bucket_arn,
                target_s3_bucket.bucket_arn + "/*",
            ]))
        target_s3_bucket.grant_write(sfCompactFunction)

        STATE_MACHINE_TEMPLATE = {
            "StartAt": "GetListofPrefixes",
            "States": {
                "GetListofPrefixes": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "Payload.$": "$",
                    "FunctionName": "${LIST_LAMBDA}:$LATEST"
                },
                "Retry": [
                    {
                    "ErrorEquals": [
                        "Lambda.ServiceException",
                        "Lambda.AWSLambdaException",
                        "Lambda.SdkClientException",
                        "Lambda.TooManyRequestsException"
                    ],
                    "IntervalSeconds": 1,
                    "MaxAttempts": 3,
                    "BackoffRate": 2
                    }
                ],
                "Next": "ForEachS3Prefix",
                "OutputPath": "$.Payload"
                },
                "ForEachS3Prefix": {
                "Type": "Map",
                "ItemProcessor": {
                    "ProcessorConfig": {
                    "Mode": "DISTRIBUTED",
                    "ExecutionType": "EXPRESS"
                    },
                    "StartAt": "CompactFilesInPrefix",
                    "States": {
                    "CompactFilesInPrefix": {
                        "Type": "Task",
                        "Resource": "arn:aws:states:::lambda:invoke",
                        "OutputPath": "$.Payload",
                        "Parameters": {
                        "Payload.$": "$",
                        "FunctionName": "${COMPACT_LAMBDA}:$LATEST"
                        },
                        "Retry": [
                        {
                            "ErrorEquals": [
                            "Lambda.ServiceException",
                            "Lambda.AWSLambdaException",
                            "Lambda.SdkClientException",
                            "Lambda.TooManyRequestsException"
                            ],
                            "IntervalSeconds": 1,
                            "MaxAttempts": 3,
                            "BackoffRate": 2
                        }
                        ],
                        "End": True
                    }
                    }
                },
                "MaxConcurrency": 100,
                "Label": "ForEachS3Prefix",
                "End": True,
                "ItemsPath": "$.s3_locations"
                }
            }
        }

        stateMachineLogGroup = logs.LogGroup(self, "CompactionStateMachineLogGroup")

        compactionStateMachine = stepfunctions.StateMachine(self, "CompactionStateMachine",
            definition_body = stepfunctions.DefinitionBody.from_string(json.dumps(STATE_MACHINE_TEMPLATE)),
            definition_substitutions={
                "LIST_LAMBDA": sfListPrefixFunction.function_arn,
                "COMPACT_LAMBDA": sfCompactFunction.function_arn
            },
            logs=stepfunctions.LogOptions(
                destination=stateMachineLogGroup,
                level=stepfunctions.LogLevel.ALL
            ),
            tracing_enabled=True
        )

        compactionStateMachine.add_to_role_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "lambda:InvokeFunction"
            ],
            resources=[
                sfCompactFunction.function_arn,
                sfListPrefixFunction.function_arn,
                sfCompactFunction.function_arn + ":*",
                sfListPrefixFunction.function_arn + ":*",
            ]))

        #Required for distributed map. Circular dependency issue means currently cannot use add_to_role_policy().
        sfDmPolicy = iam.Policy(self, "distributedMapPolicy", 
            statements = [
                iam.PolicyStatement(
                    effect = iam.Effect.ALLOW,
                    actions = [
                        "states:StartExecution"
                    ],
                    resources = [
                        compactionStateMachine.state_machine_arn
                    ]
                ),
                iam.PolicyStatement(
                    effect = iam.Effect.ALLOW,
                    actions = [
                        "states:DescribeExecution",
                        "states:StopExecution"
                    ],
                    resources = [
                        compactionStateMachine.state_machine_arn + ":*"
                    ]
                )
            ]
        )
        sfDmPolicy.attach_to_role(compactionStateMachine.role)

        sfTriggerRule = events.Rule(self, "compactionRuleStepFunction",
                            enabled=False,
                            schedule=events.Schedule.rate(
                                Duration.days(int(previous_days)))
                                )
        
        sfTriggerRule.add_target(targets.SfnStateMachine(
            machine=compactionStateMachine,
            input=events.RuleTargetInput.from_object(
                {
                    "s3_source_uri": source_s3_bucket_uri,
                    "s3_destination_uri": target_s3_bucket_uri,
                    "date_format": date_format,
                    "duration": int(previous_days)
                }
            )
        ))