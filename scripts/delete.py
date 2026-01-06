#!/usr/bin/env python3
"""
Delete S3 objects listed in a CSV with columns: bucket,key

- Uses S3 DeleteObjects API (up to 1000 keys per request) for efficiency.
- Supports --dry-run, basic retry/backoff, and optional per-bucket grouping.
- Works with AWS auth from environment, config files, or instance/role credentials.

Usage:
  python delete_s3_from_csv.py --csv objects.csv
  python delete_s3_from_csv.py --csv objects.csv --dry-run
  python delete_s3_from_csv.py --csv objects.csv --region us-west-2
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from collections import defaultdict
from typing import Dict, Iterable, List, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

BATCH_SIZE = 1000  # S3 DeleteObjects max


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True, help="Path to CSV file with columns: bucket,key")
    p.add_argument("--region", default=None, help="AWS region (optional)")
    p.add_argument("--dry-run", action="store_true", help="Print what would be deleted, but do not delete")
    p.add_argument("--max-retries", type=int, default=5, help="Max retries per batch on throttling/transient errors")
    p.add_argument("--sleep-base", type=float, default=0.5, help="Base seconds for exponential backoff")
    p.add_argument(
        "--group-by-bucket",
        action="store_true",
        help="Group deletes by bucket (uses more memory; faster/cleaner). " "If omitted, deletes stream in CSV order (lower memory).",
    )
    return p.parse_args()


def read_rows(csv_path: str) -> Iterable[Tuple[str, str]]:
    """
    Reads bucket,key pairs from a CSV with headers bucket,key
    """
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        missing = {"bucket", "key"} - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV is missing required columns: {sorted(missing)}. " f"Found columns: {reader.fieldnames}")
        for i, row in enumerate(reader, start=2):  # header is line 1
            bucket = (row.get("bucket") or "").strip()
            key = (row.get("key") or "").strip()
            if not bucket or not key:
                raise ValueError(f"Empty bucket/key at CSV line {i}: {row}")
            yield bucket, key


def chunked(items: List[Dict[str, str]], n: int) -> Iterable[List[Dict[str, str]]]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


def should_retry(err: ClientError) -> bool:
    code = err.response.get("Error", {}).get("Code", "")
    # Typical transient/throttling codes
    return code in {
        "Throttling",
        "ThrottlingException",
        "RequestLimitExceeded",
        "SlowDown",
        "InternalError",
        "ServiceUnavailable",
        "503",
    }


def delete_batch(
    s3,
    bucket: str,
    objects: List[Dict[str, str]],
    dry_run: bool,
    max_retries: int,
    sleep_base: float,
) -> Tuple[int, int]:
    """
    Returns (deleted_count, error_count)
    """
    if dry_run:
        for obj in objects:
            print(f"DRY-RUN delete s3://{bucket}/{obj['Key']}")
        return (len(objects), 0)

    attempt = 0
    while True:
        try:
            resp = s3.delete_objects(Bucket=bucket, Delete={"Objects": objects, "Quiet": False})
            deleted = resp.get("Deleted", []) or []
            errors = resp.get("Errors", []) or []

            # Print any per-key errors (don’t fail the whole run unless you want to)
            for e in errors:
                k = e.get("Key")
                code = e.get("Code")
                msg = e.get("Message")
                print(f"ERROR deleting s3://{bucket}/{k}: {code} - {msg}", file=sys.stderr)

            return (len(deleted), len(errors))

        except ClientError as e:
            attempt += 1
            if attempt > max_retries or not should_retry(e):
                print(f"FATAL batch failure on bucket={bucket} after {attempt} attempt(s): {e}", file=sys.stderr)
                raise
            sleep_s = sleep_base * (2 ** (attempt - 1))
            print(
                f"Retryable error on bucket={bucket} attempt={attempt}/{max_retries}: "
                f"{e.response.get('Error', {}).get('Code')} - backing off {sleep_s:.2f}s",
                file=sys.stderr,
            )
            time.sleep(sleep_s)


def stream_delete(csv_path: str, s3, args: argparse.Namespace) -> None:
    """
    Low-memory mode: process in CSV order, batching per bucket but not holding everything.
    If the CSV interleaves buckets, this will flush batches as buckets change.
    """
    current_bucket = None
    batch: List[Dict[str, str]] = []

    total_keys = total_deleted = total_errors = 0

    def flush():
        nonlocal batch, total_deleted, total_errors, total_keys, current_bucket
        if not batch or current_bucket is None:
            return
        d, e = delete_batch(
            s3=s3,
            bucket=current_bucket,
            objects=batch,
            dry_run=args.dry_run,
            max_retries=args.max_retries,
            sleep_base=args.sleep_base,
        )
        total_deleted += d
        total_errors += e
        batch = []

    for bucket, key in read_rows(csv_path):
        # If bucket changes, flush prior bucket batch
        if current_bucket is None:
            current_bucket = bucket
        elif bucket != current_bucket:
            flush()
            current_bucket = bucket

        batch.append({"Key": key})
        total_keys += 1

        if len(batch) >= BATCH_SIZE:
            flush()

        if total_keys % 10000 == 0:
            print(f"Progress: {total_keys} keys processed (deleted={total_deleted}, errors={total_errors})", file=sys.stderr)

    flush()
    print(f"Done. keys={total_keys}, deleted={total_deleted}, errors={total_errors}", file=sys.stderr)


def grouped_delete(csv_path: str, s3, args: argparse.Namespace) -> None:
    """
    Higher-memory mode: read all keys, group by bucket, then delete in large contiguous batches.
    """
    buckets: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    total_keys = 0

    for bucket, key in read_rows(csv_path):
        buckets[bucket].append({"Key": key})
        total_keys += 1
        if total_keys % 100000 == 0:
            print(f"Loaded {total_keys} keys...", file=sys.stderr)

    total_deleted = total_errors = 0
    for bucket, objs in buckets.items():
        print(f"Deleting {len(objs)} object(s) from bucket={bucket}", file=sys.stderr)
        for chunk in chunked(objs, BATCH_SIZE):
            d, e = delete_batch(
                s3=s3,
                bucket=bucket,
                objects=chunk,
                dry_run=args.dry_run,
                max_retries=args.max_retries,
                sleep_base=args.sleep_base,
            )
            total_deleted += d
            total_errors += e

    print(f"Done. keys={total_keys}, deleted={total_deleted}, errors={total_errors}", file=sys.stderr)


def main() -> int:
    args = parse_args()

    # Botocore retry config for additional resilience; we still do explicit backoff for certain errors.
    config = Config(
        retries={"max_attempts": max(10, args.max_retries), "mode": "standard"},
        region_name=args.region,
    )
    s3 = boto3.client("s3", config=config)

    if args.group_by_bucket:
        grouped_delete(args.csv, s3, args)
    else:
        stream_delete(args.csv, s3, args)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
