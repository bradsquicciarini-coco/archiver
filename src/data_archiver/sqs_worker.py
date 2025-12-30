"""Simple SQS worker skeleton."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
from typing import Any, Dict

import boto3
from mcap_ros1.decoder import DecoderFactory as Ros1DecoderFactory
from mcap_protobuf.decoder import DecoderFactory as ProtobufDecoderFactory
from mcap.reader import make_reader

LOGGER = logging.getLogger(__name__)

TOPICS = [
    "robot.h264_video.back",
    "robot.h264_video.front",
    "robot.h264_video.left",
    "robot.h264_video.right",
    "/camera_back/camera_info",
    "/camera_front/camera_info",
    "/camera_left/camera_info",
    "/camera_right/camera_info",
    "/acu_driver/gps_nav_topic",
    "/imu",
    "/point_one/pose",
    "/speed_fb",
    "/odom",
    "/joy/selected",
    "/cmd_vel/joystick/raw",
    "/cmd_vel",
    "/object_detection/detections_2d",
    "/object_detection/detections_3d",
]


def build_sqs_client() -> Any:
    return boto3.client(
        "sqs",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        endpoint_url=os.getenv("SQS_ENDPOINT_URL"),
    )


def build_s3_client() -> Any:
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    )


def logged_cmd(cmd: str):
    LOGGER.info(cmd)
    subprocess.call(cmd, shell=True)


def get_mcap_timing(filepath: str):
    with open(filepath, "rb") as f:
        reader = make_reader(f)
        summary = reader.get_summary()
        start_s = int(summary.statistics.message_start_time / 1e9)
        end_s = int(summary.statistics.message_end_time / 1e9)
        duration_s = end_s - start_s
        return start_s, end_s, duration_s


def infer_additional_vehicle_metadata(filepath: str):

    with open(filepath, "rb") as f:
        reader = make_reader(f, decoder_factories=[Ros1DecoderFactory(), ProtobufDecoderFactory()])
        summary = reader.get_summary()
        has_new_gps = "/point_one/pose" in [chan.topic for cid, chan in summary.channels.items()]
        has_new_front_cam = None
        has_new_left_cam = None
        has_new_right_cam = None
        vehicle_model = "1"
        camera_version = "1"

        def is_new_cam(metadata):
            for m in metadata:
                if m.key == "codedWidth":
                    return int(m.value) == 1920

        for _, chan, _, dmsg in reader.iter_decoded_messages(topics=["robot.h264_video.front", "robot.h264_video.right", "robot.h264_video.left"]):
            if not dmsg.keyframe:
                continue
            if chan.topic == "robot.h264_video.front" and has_new_front_cam is not None:
                has_new_front_cam = is_new_cam(dmsg.metadata)
            elif chan.topic == "robot.h264_video.right" and has_new_right_cam is not None:
                has_new_right_cam = is_new_cam(dmsg.metadata)
            elif chan.topic == "robot.h264_video.left" and has_new_left_cam is not None:
                has_new_left_cam = is_new_cam(dmsg.metadata)

            checked_all = all([v is not None for v in [has_new_front_cam, has_new_right_cam, has_new_left_cam]])
            if checked_all:
                if any([has_new_front_cam, has_new_right_cam, has_new_left_cam]):
                    vehicle_model = "1.5"
                if all([has_new_front_cam, has_new_right_cam, has_new_left_cam]):
                    camera_version = "1.5-3"
                elif has_new_front_cam and (not has_new_right_cam and not has_new_left_cam):
                    camera_version = "1.5-1"
                break

        return dict(
            vehicle_model=vehicle_model,
            camera_version=camera_version,
            gps_version="2" if has_new_gps else "1",
        )


def process_message(body: str, attributes: Dict[str, Any], tmp_dir: Path, keep_bags: bool) -> None:
    """Replace this with your real work."""
    payload = json.loads(body) if body.strip().startswith("{") else {"body": body}
    LOGGER.info("processing assignment_id=%s attrs=%s", payload.get("pilot_assignment_id"), attributes)

    # ------------- Data prep ----------------------
    LOGGER.info("using tmp_dir=%s", tmp_dir)
    s3 = build_s3_client()

    # 1. download all logs for a given trip
    # TODO(Brad): do this
    # TODO(Brad): how should we handle errors to download any files?. Should probably reject if any fail
    bucket = "coco-gg-bags-prod"
    local_files = []
    log_files = payload.get("log_files") or []
    LOGGER.info("will download %d files", len(log_files))
    for key in log_files:
        s3_uri = f"s3://{bucket}/{key}"
        output_fp = tmp_dir / key.split("/")[-1]
        if output_fp.exists():
            LOGGER.info("skipping download; already exists %s", output_fp)
            local_files.append(output_fp)
            continue
        try:
            LOGGER.info("downloading %s -> %s", s3_uri, output_fp)
            s3.download_file(bucket, key, str(output_fp))
        except Exception as exc:
            LOGGER.exception("failed to download %s -> %s", s3_uri, output_fp)
            raise RuntimeError(f"failed to download {s3_uri} -> {output_fp}") from exc
        local_files.append(output_fp)

    # 2. convert any bags to mcap (prob just use mcap cli)
    converted_files = []
    for local_fp in local_files:
        base_filepath, ext = os.path.splitext(local_fp)
        if ext != ".bag":
            converted_files.append(local_fp)
            continue
        LOGGER.info(f"Will convert {local_fp} to an mcap")
        input_fp = local_fp
        output_fp = f"{base_filepath}.mcap"

        # TODO(Brad): check it succeeds
        if os.path.exists(output_fp):
            LOGGER.info(f"mcap for bag: {input_fp} already exists. skipping ...")
        else:
            cmd = f"mcap convert {input_fp} {output_fp}"
            logged_cmd(cmd)

        # cleanup
        if not keep_bags:
            LOGGER.info(f"removing {input_fp}")
            os.remove(input_fp)
        converted_files.append(output_fp)
    local_files = converted_files

    # by this point we should have a temporary directory with the following structure:
    # <tmp>
    #    <name0>.mcap
    #    ......
    #    <nameN>.mcap
    #
    #  We need to add a few more things:
    #   1) we need to create a new mcap with map issues
    #   2) create a new mcap with geojson routes that have been clipped

    # 3. inject any additional data (e.g. routes and map issues)
    # ... TODO(Brad): map issues
    # ... TODO(Brad): inject camera calibration (and tfs?)
    # ... TODO(Brad): routes (need to clip) according to origin / destination

    # 4. Combine data into single mcap for the assignment
    # ... merge
    merged_output_fp = str(tmp_dir / "merged.mcap")
    cmd = f'mcap merge {" ".join([str(p) for p in local_files])} -o {merged_output_fp}'
    logged_cmd(cmd)

    # ... filter for specific topics and times
    filtered_output_fp = str(tmp_dir / "final.mcap")
    valid_start_s = int(payload["valid_start"])
    valid_end_s = int(payload["valid_end"])
    include_str = "".join([f' -y "{t}"   ' for t in TOPICS])
    cmd = f"mcap filter {merged_output_fp} {include_str} -s {valid_start_s} -e {valid_end_s} -o {filtered_output_fp}"
    logged_cmd(cmd)

    # 5. infer additional metadata
    # ... check for existance of /point_one/pose
    # ... check resolution of left/front/right cameras
    existing_metadata = payload.get("external_metadata")
    start_s, end_s, duration_s = get_mcap_timing(filtered_output_fp)
    print(start_s, end_s, duration_s)
    existing_metadata["clip_start_utc"] = datetime.fromtimestamp(start_s).isoformat() + "Z"
    existing_metadata["clip_end_utc"] = datetime.fromtimestamp(end_s).isoformat() + "Z"
    existing_metadata["clip_duration_seconds"] = duration_s
    aux_metadata = infer_additional_vehicle_metadata(filtered_output_fp)
    existing_metadata["vehicle"].update(aux_metadata)
    LOGGER.info("parsed metadata=%s", json.dumps(existing_metadata))

    # 6. quality check
    # ... check that all topics are there
    # TODO(Brad): do this


def poll_loop(queue_url: str, *, max_messages: int, wait_time: int, visibility_timeout: int, once: bool, tmp_dir: Path, keep_bags: bool) -> None:
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
                process_message(msg.get("Body", ""), msg.get("MessageAttributes", {}), tmp_dir, keep_bags=keep_bags)
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
            except Exception:
                LOGGER.exception("message processing failed; leaving in queue")

        if once:
            return


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SQS worker skeleton")
    parser.add_argument("--queue-url", required=True, help="SQS queue URL")
    parser.add_argument("--max-messages", type=int, default=1, help="Messages per poll")
    parser.add_argument("--wait-time", type=int, default=10, help="Long poll wait time (seconds)")
    parser.add_argument("--visibility-timeout", type=int, default=30, help="Visibility timeout (seconds)")
    parser.add_argument("--once", action="store_true", help="Process a single poll and exit")
    parser.add_argument("--keep-bags", action="store_true")
    parser.add_argument("--tmp-dir", help="Override temp directory path")
    parser.add_argument(
        "--persist-tmp",
        action="store_true",
        help="Use a persistent temp directory under ./.tmp/data_archiver",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args()
    if args.persist_tmp:
        tmp_dir = Path(".tmp") / "data_archiver"
    elif args.tmp_dir:
        tmp_dir = Path(args.tmp_dir)
    else:
        tmp_dir = Path(tempfile.gettempdir())
    tmp_dir.mkdir(parents=True, exist_ok=True)
    poll_loop(
        args.queue_url,
        max_messages=args.max_messages,
        wait_time=args.wait_time,
        visibility_timeout=args.visibility_timeout,
        once=args.once,
        tmp_dir=tmp_dir,
        keep_bags=args.keep_bags,
    )
    return 0
