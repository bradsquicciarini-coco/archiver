"""Create a localstack SQS queue and send a test message."""

import os

import boto3


def main() -> None:
    sqs = boto3.client(
        "sqs",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        endpoint_url=os.getenv("SQS_ENDPOINT_URL", "http://localhost:4566"),
    )

    queue_name = os.getenv("QUEUE_NAME", "localstack-demo-queue")
    create_resp = sqs.create_queue(QueueName=queue_name)
    queue_url = create_resp["QueueUrl"]

    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody='{"hello": "world"}',
        MessageAttributes={
            "source": {"DataType": "String", "StringValue": "localstack"}
        },
    )

    print(queue_url)
