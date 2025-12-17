#!/usr/bin/env python3
import argparse
import os
import queue
import threading
import time

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("bucket")
    parser.add_argument("--out", default="inventory.parquet")
    parser.add_argument("--workers", type=int, default=64)
    parser.add_argument("--profile")
    parser.add_argument("--rows-per-file", type=int, default=100000)
    args = parser.parse_args()
    os.makedirs(args.out, exist_ok=True)
    done_path = os.path.join(args.out, ".done")
    todo_path = os.path.join(args.out, ".todo")
    done = set(open(done_path).read().splitlines()) if os.path.exists(done_path) else set()
    todo = set(open(todo_path).read().splitlines()) if os.path.exists(todo_path) else set()
    todo.add("")
    q: queue.Queue[str | None] = queue.Queue()
    for prefix in todo:
        if prefix not in done:
            q.put(prefix)
    outq: queue.Queue[list[str] | None] = queue.Queue()
    seen: set[str] = set(todo) | done
    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    s3 = session.client("s3")
    bar = tqdm(unit="obj", dynamic_ncols=True)
    lock = threading.Lock()

    def writer() -> None:
        buf: list[str] = []
        idx = 0
        while True:
            batch = outq.get()
            if batch is None:
                outq.task_done()
                break
            buf.extend(batch)
            bar.update(len(batch))
            while len(buf) >= args.rows_per_file:
                table = pa.Table.from_arrays([pa.array(buf[: args.rows_per_file])], names=["key"])
                pq.write_table(table, os.path.join(args.out, f"part-{idx}.parquet"))
                buf = buf[args.rows_per_file :]
                idx += 1
            outq.task_done()
        if buf:
            table = pa.Table.from_arrays([pa.array(buf)], names=["key"])
            pq.write_table(table, os.path.join(args.out, f"part-{idx}.parquet"))

    def worker() -> None:
        while True:
            prefix = q.get()
            if prefix is None:
                q.task_done()
                break
            if prefix and prefix in done:
                q.task_done()
                continue
            token = None
            while True:
                kwargs = {"Bucket": args.bucket, "Prefix": prefix, "Delimiter": "/"}
                if token:
                    kwargs["ContinuationToken"] = token
                resp = s3.list_objects_v2(**kwargs)
                if "Contents" in resp:
                    outq.put([o["Key"] for o in resp["Contents"]])
                with lock:
                    for cp in resp.get("CommonPrefixes", []):
                        sub = cp["Prefix"]
                        if sub not in seen:
                            seen.add(sub)
                            q.put(sub)
                            with open(todo_path, "a") as f:
                                f.write(sub + "\n")
                token = resp.get("NextContinuationToken")
                if not token:
                    break
            if prefix:
                with lock:
                    if prefix not in done:
                        with open(done_path, "a") as f:
                            f.write(prefix + "\n")
                        done.add(prefix)
            bar.set_postfix(queue=q.qsize(), done=len(done))
            q.task_done()

    wt = threading.Thread(target=writer, daemon=True)
    wt.start()
    workers = [threading.Thread(target=worker, daemon=True) for _ in range(args.workers)]
    for t in workers:
        t.start()
    q.join()
    outq.join()
    for _ in workers:
        q.put(None)
    for t in workers:
        t.join()
    outq.put(None)
    outq.join()
    wt.join()
    bar.close()


if __name__ == "__main__":
    main()
