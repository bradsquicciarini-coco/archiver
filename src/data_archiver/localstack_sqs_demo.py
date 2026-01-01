"""Create a localstack SQS queue and send a test message."""

import json
import os
import sys

import boto3
from tqdm import tqdm


def main() -> None:
    sqs = boto3.client(
        "sqs",
        region_name=os.getenv("AWS_REGION", "us-west-2"),
        # endpoint_url=os.getenv("SQS_ENDPOINT_URL", "http://localhost:4566"),
    )

    queue_name = os.getenv("QUEUE_NAME", "localstack-demo-queue")
    create_resp = sqs.create_queue(QueueName=queue_name)
    queue_url = create_resp["QueueUrl"]

    input_path = sys.argv[1]
    with open(input_path, "r") as f:
        for line in tqdm(f, desc="Sending messages", unit="msg"):
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(payload),
                MessageAttributes={"source": {"DataType": "String", "StringValue": "localstack"}},
            )

    print(queue_url)


if __name__ == "__main__":
    main()
