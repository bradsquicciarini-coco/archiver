import argparse
import csv
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import time
import boto3
import botocore
from mcap.reader import make_reader
import json_log_formatter


logger = logging.getLogger(__name__)


class LevelJsonFormatter(json_log_formatter.JSONFormatter):
    def json_record(self, message: str, extra: dict, record: logging.LogRecord) -> dict:
        extra["level"] = record.levelname
        extra["logger"] = record.name
        return super().json_record(message, extra, record)


def build_sqs_client():
    return boto3.client(
        "sqs",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        endpoint_url=os.getenv("SQS_ENDPOINT_URL"),
    )


def build_s3_client() -> boto3.client:
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    )


def logged_cmd(cmd: str, quiet: bool = False, check=True):
    logger.debug(cmd)
    subprocess.run(cmd, shell=True, check=check, stdout=subprocess.DEVNULL if quiet else None, stderr=subprocess.DEVNULL if quiet else None)


def fix_mcap(filepath):
    channels = [
        "/camera_back/camera_info:protobuf",
        "/camera_left/camera_info:protobuf",
        "/camera_right/camera_info:protobuf",
        "/camera_front/camera_info:protobuf",
        "/tf_static:protobuf",
    ]
    cmd = f"mcap-filter -in {filepath} -out {filepath}.fixed {' -keep-channel '.join(channels)}"
    logged_cmd(cmd)
    logged_cmd(f"mv {filepath}.fixed {filepath}")


def make_openai_metadata_file(filepath, metadata):

    def prepare_metadata(metadata: dict) -> dict:
        return {
            "reference_id": metadata["reference_id"],
            "version": int(metadata["__version"]),
            "supplemental": {
                "vehicle": {
                    "vehicle_id": metadata["vehicle__vehicle_id"],
                    "vehicle_model": metadata["vehicle__vehicle_model"],
                    "vehicle_age_days": int(metadata["vehicle__vehicle_age_days"]),
                    "gps_version": metadata["vehicle__gps_version"],
                    "camera_version": metadata["vehicle__camera_version"],
                },
                "clip_avg_speed": float(metadata["clip_avg_speed"]),
                "weather": {
                    "weather_icon": metadata["weather__weather_icon"],
                    "precipitation_mm": float(metadata["weather__precipitation_mm"]),
                    "temperature_c": float(metadata["weather__temperature_c"]),
                    "cloud_cover": float(metadata["weather__cloud_cover"]),
                    "time_of_day": metadata["weather__time_of_day"],
                },
                "location": {
                    "city": metadata["location__city"],
                    "zone": metadata["location__zone"],
                    "local_timezone": metadata["location__local_timezone"],
                    "country": "USA",
                },
                "trip_id": metadata["trip_id"],
                "clip_start_utc": metadata["clip_start_utc"],
                "clip_end_utc": metadata["clip_end_utc"],
            },
        }

    output_filepath = f"{filepath}.metadata.json"
    external_metadata = prepare_metadata(metadata)
    with open(output_filepath, "w") as f:
        json.dump(external_metadata, f)


def quality_check(filepath):
    with open(filepath, "rb") as f:
        reader = make_reader(f)
        summary = reader.get_summary()

        cam_cid = None
        odom_cid = None
        for cid, chan in summary.channels.items():
            if chan.topic == "robot.h264_video.front":
                cam_cid = cid
            if chan.topic == "/odom":
                odom_cid = cid

        if cam_cid is None:
            return False, "cam not present"

        if odom_cid is None:
            return False, "odom not present"

        start_s = int(summary.statistics.message_start_time / 1e9)
        end_s = int(summary.statistics.message_end_time / 1e9)
        duration_s = end_s - start_s
        cam_hz = summary.statistics.channel_message_counts[cam_cid] / duration_s
        odom_hz = summary.statistics.channel_message_counts[odom_cid] / duration_s

        if cam_hz < 18:
            return False, f"cam below expected rate: {cam_hz}"

        if odom_hz < 30:
            return False, f"odom below expected rate: {odom_hz}"
    return True, None


def s3_key_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def cleanup(tmp_dir):
    for file in os.listdir(tmp_dir):
        fp = os.path.join(tmp_dir, file)
        logger.debug(f"Removing {fp}")
        os.remove(fp)
    logger.debug(f"Removing {tmp_dir}")
    os.rmdir(tmp_dir)


def process_message(s3, body: str, tmp_dir_base: Path):
    start_time = time.monotonic()
    logger.debug(f"received: {body}")
    payload = json.loads(body)
    tar_name = payload["name"]
    logger.info(f"Processing {tar_name}")

    # 1) parse metadata from the sqs message
    bucket = payload["bucket"]
    keys = payload["keys"]
    fixes = payload["fixes"]
    output_key = f"tar/{tar_name}"

    exists = s3_key_exists(s3, bucket, output_key)
    if exists:
        logger.info(f"s3://{bucket}/{output_key} exists will not process")
        return

    basename, _ = os.path.splitext(tar_name)
    tmp_dir = tmp_dir_base / str(basename)
    tmp_dir.mkdir(exist_ok=True)

    # 2) for each file:
    local_files = []
    success, failures = [], []
    logger.info(f"Will download {len(keys)} files")
    for key in keys:
        # ... get metadata for object
        resp = s3.head_object(Bucket=bucket, Key=key)
        metadata = resp.get("Metadata", {})

        # ... download
        reference_id = metadata["reference_id"]
        output_fp = tmp_dir / f"{reference_id}.mcap"
        if not output_fp.exists():
            logger.debug(f"Download s3://{bucket}/{key}")
            s3.download_file(bucket, key, output_fp)
        else:
            logger.debug(f"{output_fp} exists. Skipping...")

        # ... quality check
        ok, err = quality_check(output_fp)
        if not ok:
            logger.warning(f"{key} failed quality check: {err}. Will not include in tar.")
            failures.append(reference_id)
            continue
        else:
            success.append(reference_id)

        # ... and replace with correct data if None (can in include in sqs queue)
        if fix := fixes.get(key):
            logger.info(f"Applying metadata fix: {fix}")
            logger.debug(f"Metadata before fix: {metadata}")
            metadata.update(fix)
            logger.debug(f"Metadata after fix: {metadata}")

        # ... fix schema problem
        logger.debug(f"Fixing schema for {output_fp}")
        fix_mcap(output_fp)

        # ... make metadata file for openai
        make_openai_metadata_file(output_fp, metadata)

        local_files.append(output_fp)

    logger.info(f"Will add {len(local_files)} out of the {len(keys)} we started with.")

    # 4) write out to a tar file
    tarfile_fp = tmp_dir / tar_name
    logger.info(f"Making tarfile {tarfile_fp}")
    with tarfile.open(tarfile_fp, "w") as tar:
        for p in local_files:
            p = Path(p)
            tar.add(p, arcname=p.name)
            metadata_fp = p.with_name(f"{p.name}.metadata.json")
            tar.add(metadata_fp, arcname=metadata_fp.name)
            # Remove local files to reduce disk usage after they are archived.
            p.unlink(missing_ok=True)
            metadata_fp.unlink(missing_ok=True)

    # 6) manifest
    manifest_fp = tmp_dir / f"{basename}.csv"
    with open(manifest_fp, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["pilot_assignment_id", "success"])
        for r in success:
            writer.writerow([r, 1])
        for r in failures:
            writer.writerow([r, 0])

    # 5) upload to s3://coco-trip-clips-976053906881-us-west-2/tar/<name of tar file> (maybe put files inside in metdata)
    logger.info("Uploading manifest")
    s3.upload_file(manifest_fp, bucket, f"tar-manifest/{basename}.csv")
    logger.info(f"Uploading {output_key}")
    s3.upload_file(tarfile_fp, bucket, output_key)

    # 6) clean up
    cleanup(tmp_dir)
    elapsed_s = time.monotonic() - start_time
    logger.info(f"Completed {tar_name} in {elapsed_s:.2f}s")


def main():
    parser = argparse.ArgumentParser(description="Minimal SQS consumer")
    parser.add_argument("--queue-url", default=os.getenv("QUEUE_URL"), help="SQS queue URL (or set QUEUE_URL)")
    parser.add_argument("--region", default="us-west-2", help="AWS region")
    parser.add_argument("--max-messages", type=int, default=1, help="Messages per poll (1-10)")
    parser.add_argument("--wait-time", type=int, default=10, help="Long poll wait time (seconds)")
    parser.add_argument("--visibility-timeout", type=int, default=3600, help="Visibility timeout (seconds)")
    parser.add_argument("--once", action="store_true", help="Poll once and exit")
    parser.add_argument("--test-msg", type=str)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    # logger
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    if args.debug:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        logging.basicConfig(handlers=[handler])
        logger.setLevel(log_level)
    else:
        handler = logging.StreamHandler()
        handler.setFormatter(LevelJsonFormatter())
        logging.basicConfig(handlers=[handler])
        logger.setLevel(log_level)

    s3 = build_s3_client()
    sqs = build_sqs_client()

    # tmp dir
    if args.debug:
        tmp_dir = Path(".tmp") / "tar_worker"
    else:
        tmp_dir = Path(tempfile.gettempdir())
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # test msg
    if args.test_msg:
        test_msg_fp = Path(args.test_msg)
        logger.info(f"Loading test msg: {test_msg_fp}")
        with test_msg_fp.open() as r:
            msg = r.readline()
            process_message(s3, msg, tmp_dir_base=tmp_dir)
        sys.exit(0)

    # main loop
    while True:
        resp = sqs.receive_message(
            QueueUrl=args.queue_url,
            MaxNumberOfMessages=args.max_messages,
            WaitTimeSeconds=args.wait_time,
            VisibilityTimeout=args.visibility_timeout,
        )

        messages = resp.get("Messages", [])
        if not messages:
            if args.once:
                return
            continue

        for msg in messages:
            try:
                body = msg["Body"]
                receipt = msg["ReceiptHandle"]
                process_message(s3, body, tmp_dir_base=tmp_dir)
                sqs.delete_message(QueueUrl=args.queue_url, ReceiptHandle=receipt)
            except Exception:
                logger.exception("message processing failed; leaving in queue")

        if args.once:
            return


if __name__ == "__main__":
    main()
