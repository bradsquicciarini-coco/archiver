import argparse
import json
import os
import boto3
import duckdb
from typing import List, Tuple
import pandas as pd


def load_data(filepath: str):
    con = duckdb.connect()
    query = f"""
        select
            user_metadata->>'location__city' as city,
            user_metadata->>'reference_id' as pilot_assignment_id,
            user_metadata->>'vehicle__camera_version' as camera_version,
            user_metadata->>'weather__weather_icon' as weather,
            key,
            size / (1024*1024*1024::bigint) as size_gb,
            (user_metadata->>'clip_duration_seconds')::double / 3600.0 as clip_duration,
            (user_metadata->>'clip_start_utc')::timestamp as clip_start_utc,
            date_part('month', clip_start_utc) as month,
            user_metadata
        from '{filepath}';
    """
    return con.execute(query).df()


def pack_by_size(df: pd.DataFrame, size_target: float, size_col: str = "size_gb") -> Tuple[List[List[int]], List[float]]:
    if size_target <= 0:
        raise ValueError("size_tb_target must be > 0")
    if size_col not in df.columns:
        raise KeyError(f"missing column: {size_col}")

    d = df.copy()
    d[size_col] = pd.to_numeric(d[size_col], errors="raise")
    if (d[size_col] < 0).any():
        raise ValueError(f"{size_col} contains negative values")

    # Sort but KEEP original index values
    d = d.sort_values(size_col, ascending=False)

    groups: List[List[int]] = []
    group_sums: List[float] = []

    for idx, sz in zip(d.index.tolist(), d[size_col].tolist()):
        placed = False
        for g, cur in enumerate(group_sums):
            if cur + sz <= size_target:
                groups[g].append(idx)  # <-- original df index
                group_sums[g] = cur + sz
                placed = True
                break
        if not placed:
            groups.append([idx])
            group_sums.append(sz)

    return groups, group_sums


def load_metadata_fixes(filepath):
    con = duckdb.connect()
    query = f"""
        select
            key,
            weather__precipitation_mm,
            weather__temperature_c,
            weather__weather_icon,
            weather__cloud_cover
        from '{filepath}';
    """
    mf_df = con.execute(query).df()
    metadata_fixes = mf_df.set_index("key").to_dict(orient="index")
    return metadata_fixes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, help="input parqeut", required=True)
    parser.add_argument("--metadata-file", type=str, help="", required=True)
    parser.add_argument("--month", type=int, help="month to build", required=True)
    args = parser.parse_args()

    kwargs = dict(region_name=os.getenv("AWS_REGION", "us-west-2"))
    if endpoint := os.getenv("SQS_ENDPOINT_URL"):
        kwargs["endpoint_url"] = endpoint
    sqs = boto3.client("sqs", **kwargs)

    queue_name = os.getenv("QUEUE_NAME", "localstack-demo-queue")
    create_resp = sqs.create_queue(QueueName=queue_name)
    queue_url = create_resp["QueueUrl"]
    print(f"{queue_url}")

    TARGET_SIZE_GB = 100

    # 1. bucket into appropiately sized chunks
    df = load_data(args.input)
    sampled_df = df[df.month == args.month]
    groups, group_nums = pack_by_size(sampled_df[sampled_df.month == args.month], size_target=TARGET_SIZE_GB, size_col="size_gb")
    print(group_nums)
    print("Printing first group...")

    # 3. load metadata
    metadata_fixes = {}
    if args.metadata_file is not None:
        metadata_fixes = load_metadata_fixes(args.metadata_file)

    # 2. build up sqs messages
    for group_num in range(len(groups)):
        tar_name = f"2025-{args.month:02d}-{group_num:08d}.tar"
        keys = []
        fixes = {}
        for idx in groups[group_num]:
            row = sampled_df.loc[idx]
            keys.append(row.key)
            if fix := metadata_fixes.get(row.key):
                fixes[row.key] = fix
        msg = dict(name=tar_name, keys=keys, fixes=fixes, bucket="coco-trip-clips-976053906881-us-west-2")
        print(json.dumps(msg))
        break


if __name__ == "__main__":
    main()
