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

    input_path = sys.argv[1]
    with open(input_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(payload),
                MessageAttributes={
                    "source": {"DataType": "String", "StringValue": "localstack"}
                },
            )

    print(queue_url)


if __name__ == "__main__":
    main()
