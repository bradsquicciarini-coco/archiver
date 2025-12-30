"""Create a localstack SQS queue and send a test message."""

import json
import os
import sys

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

    with open(sys.argv[1], "r") as f:
        example_payload = json.load(f)

    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(example_payload),
        MessageAttributes={"source": {"DataType": "String", "StringValue": "localstack"}},
    )

    print(queue_url)


if __name__ == "__main__":
    main()
