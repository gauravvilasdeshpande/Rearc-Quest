from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_sqs as sqs,
    aws_s3_notifications as s3n
)
from constructs import Construct

class RearcDataPipelineStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        bucket = s3.Bucket(self, "RearcDataBucket", versioned=True)

        # Lambda for part 1 and 2
        sync_lambda = _lambda.Function(
            self, "SyncLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="sync_bls_lambda.handler",
            code=_lambda.Code.from_asset("lambda")
        )


        events.Rule(
            self, "DailySyncRule",
            schedule=events.Schedule.rate(duration=cdk.Duration.days(1)),
            targets=[targets.LambdaFunction(sync_lambda)]
        )


        queue = sqs.Queue(self, "ReportQueue")

        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(queue),
            s3.NotificationKeyFilter(suffix=".json")
        )

        # Analytics Lambda part 3
        analytics_lambda = _lambda.Function(
            self, "AnalyticsLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="analytics_lambda.handler",
            code=_lambda.Code.from_asset("lambda")
        )

        queue.grant_consume_messages(analytics_lambda)
        analytics_lambda.add_event_source_mapping("SQSMapping", event_source_arn=queue.queue_arn)
