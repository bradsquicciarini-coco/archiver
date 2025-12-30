"""Simple SQS worker skeleton."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from typing import Any, Dict

import boto3


def build_sqs_client() -> Any:
    return boto3.client(
        "sqs",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        endpoint_url=os.getenv("SQS_ENDPOINT_URL"),
    )


def process_message(body: str, attributes: Dict[str, Any]) -> None:
    """Replace this with your real work."""
    payload = json.loads(body) if body.strip().startswith("{") else {"body": body}
    print(f"processing: {payload} with attrs={attributes}")


def poll_loop(
    queue_url: str,
    *,
    max_messages: int,
    wait_time: int,
    visibility_timeout: int,
    once: bool,
) -> None:
    sqs = build_sqs_client()
    should_stop = False

    def handle_signal(_signum: int, _frame: Any) -> None:
        nonlocal should_stop
        should_stop = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    while not should_stop:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
            VisibilityTimeout=visibility_timeout,
            MessageAttributeNames=["All"],
        )

        messages = resp.get("Messages", [])
        if not messages:
            if once:
                return
            time.sleep(1)
            continue

        for msg in messages:
            receipt = msg["ReceiptHandle"]
            try:
                process_message(msg.get("Body", ""), msg.get("MessageAttributes", {}))
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
            except Exception:
                print("message processing failed; leaving in queue", file=sys.stderr)

        if once:
            return


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SQS worker skeleton")
    parser.add_argument("--queue-url", required=True, help="SQS queue URL")
    parser.add_argument("--max-messages", type=int, default=1, help="Messages per poll")
    parser.add_argument("--wait-time", type=int, default=10, help="Long poll wait time (seconds)")
    parser.add_argument("--visibility-timeout", type=int, default=30, help="Visibility timeout (seconds)")
    parser.add_argument("--once", action="store_true", help="Process a single poll and exit")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    poll_loop(
        args.queue_url,
        max_messages=args.max_messages,
        wait_time=args.wait_time,
        visibility_timeout=args.visibility_timeout,
        once=args.once,
    )
    return 0
