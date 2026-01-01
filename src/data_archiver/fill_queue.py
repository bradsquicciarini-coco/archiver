"""Create a localstack SQS queue and send messages from a JSONL file."""

import json
import os
import sys
import threading
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

import boto3
from tqdm import tqdm


_THREAD_LOCAL = threading.local()


def _get_sqs_client(kwargs: dict[str, str]) -> "boto3.client":
    client = getattr(_THREAD_LOCAL, "sqs_client", None)
    if client is None:
        client = boto3.client("sqs", **kwargs)
        _THREAD_LOCAL.sqs_client = client
    return client


def _load_progress(progress_path: str) -> tuple[int, int]:
    if not os.path.exists(progress_path):
        return 0, -1
    with open(progress_path, "r") as f:
        data = json.load(f)
    return int(data.get("offset", 0)), int(data.get("line", -1))


def _save_progress(progress_path: str, offset: int, line: int) -> None:
    tmp_path = f"{progress_path}.tmp"
    with open(tmp_path, "w") as f:
        json.dump({"offset": offset, "line": line}, f)
    os.replace(tmp_path, progress_path)


def _send_message_batch(
    kwargs: dict[str, str],
    queue_url: str,
    batch: list[tuple[int, int, dict]],
) -> list[tuple[int, int]]:
    sqs = _get_sqs_client(kwargs)
    entries = []
    for line_no, _, payload in batch:
        entries.append(
            {
                "Id": str(line_no),
                "MessageBody": json.dumps(payload),
                "MessageAttributes": {
                    "source": {"DataType": "String", "StringValue": "localstack"}
                },
            }
        )
    response = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
    failed = response.get("Failed", [])
    if failed:
        failed_ids = [item.get("Id", "<unknown>") for item in failed]
        raise RuntimeError(f"Failed to send batch entries: {', '.join(failed_ids)}")
    return [(line_no, offset_after) for line_no, offset_after, _ in batch]


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: fill_queue.py <jsonl_path>")

    kwargs = dict(region_name=os.getenv("AWS_REGION", "us-west-2"))
    if endpoint := os.getenv("SQS_ENDPOINT_URL"):
        kwargs["endpoint_url"] = endpoint

    sqs = boto3.client("sqs", **kwargs)

    queue_name = os.getenv("QUEUE_NAME", "localstack-demo-queue")
    create_resp = sqs.create_queue(QueueName=queue_name)
    queue_url = create_resp["QueueUrl"]

    input_path = sys.argv[1]
    progress_path = os.getenv("QUEUE_PROGRESS_PATH", f"{input_path}.progress.json")
    max_workers = max(1, int(os.getenv("QUEUE_PARALLELISM", "8")))
    max_in_flight = max(1, int(os.getenv("QUEUE_MAX_IN_FLIGHT", str(max_workers * 4))))
    batch_size = max(1, min(10, int(os.getenv("QUEUE_BATCH_SIZE", "10"))))

    offset, last_line = _load_progress(progress_path)
    next_line = last_line + 1

    completed: dict[int, int] = {}
    in_flight: dict = {}

    with open(input_path, "r") as f, ThreadPoolExecutor(max_workers=max_workers) as executor:
        if offset:
            f.seek(offset)

        pbar = tqdm(desc="Sending messages", unit="msg")
        eof = False
        batch: list[tuple[int, int, dict]] = []
        while not eof or in_flight or batch:
            while not eof and len(in_flight) < max_in_flight:
                line = f.readline()
                if not line:
                    eof = True
                    break
                offset_after = f.tell()
                line_no = next_line
                next_line += 1
                stripped = line.strip()
                if not stripped:
                    completed[line_no] = offset_after
                    pbar.update(1)
                    continue
                payload = json.loads(stripped)
                batch.append((line_no, offset_after, payload))
                if len(batch) >= batch_size:
                    future = executor.submit(_send_message_batch, kwargs, queue_url, batch)
                    in_flight[future] = None
                    batch = []

            if eof and batch:
                future = executor.submit(_send_message_batch, kwargs, queue_url, batch)
                in_flight[future] = None
                batch = []

            if in_flight:
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for future in done:
                    in_flight.pop(future)
                    for line_no, offset_after in future.result():
                        completed[line_no] = offset_after
                        pbar.update(1)

            while (last_line + 1) in completed:
                last_line += 1
                offset = completed.pop(last_line)
                _save_progress(progress_path, offset, last_line)

        pbar.close()

    print(queue_url)


if __name__ == "__main__":
    main()
