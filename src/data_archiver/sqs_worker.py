"""Simple SQS worker skeleton."""

from __future__ import annotations

import argparse
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from typing import Any, Dict

import boto3
import botocore
import json_log_formatter
import numpy as np
from shapely import wkb
from mcap_ros1.decoder import DecoderFactory as Ros1DecoderFactory
from mcap_protobuf.decoder import DecoderFactory as ProtobufDecoderFactory
from mcap.reader import make_reader

from data_archiver.enrich.camera_calibration import write_out_camera_calibration
from data_archiver.enrich.map_issue import write_out_map_issues
from data_archiver.enrich.route import find_valid_start_end_from_trace, write_out_geo_debug, write_out_routes
from data_archiver.utils.overlap import find_best_overlap, parse_timestamp_from_mcap

logger = logging.getLogger(__name__)
pilot_assignment_id_ctx: ContextVar[str | None] = ContextVar("pilot_assignment_id", default=None)


class LevelJsonFormatter(json_log_formatter.JSONFormatter):
    def json_record(self, message: str, extra: dict, record: logging.LogRecord) -> dict:
        extra["level"] = record.levelname
        extra["logger"] = record.name
        extra["pilot_assignment_id"] = getattr(record, "pilot_assignment_id", None)
        return super().json_record(message, extra, record)


class PilotAssignmentFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        pilot_assignment_id = pilot_assignment_id_ctx.get()
        record.pilot_assignment_id = pilot_assignment_id or "-"
        return True


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
    "/route/issue_report",
    "/route/geojson",
    "/tf_static",
    # debug
    "/debug/pts",
]


def build_sqs_client() -> Any:
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


def get_mcap_timing(filepath: str):
    with open(filepath, "rb") as f:
        reader = make_reader(f)
        summary = reader.get_summary()
        start_s = int(summary.statistics.message_start_time / 1e9)
        end_s = int(summary.statistics.message_end_time / 1e9)
        duration_s = end_s - start_s
        return start_s, end_s, duration_s


def recover_mcap(filepath: str):
    logger.info(f"Recovering {filepath}")
    recovered_fp = f"{filepath}.recover"
    logged_cmd(f"mcap recover {filepath} -o {recovered_fp}", check=False)
    shutil.move(recovered_fp, filepath)


def ensure_video_mcaps_readable(video_files: list[str]):
    for video_fp in video_files:
        try:
            get_mcap_timing(video_fp)
        except Exception:
            logger.warning("mcap timing failed; attempting recovery for %s", video_fp)
            recover_mcap(video_fp)
            try:
                get_mcap_timing(video_fp)
            except Exception as exc:
                raise RuntimeError(f"mcap recover failed for {video_fp}") from exc


def determine_gps_type(filepath: str):
    with open(filepath, "rb") as f:
        reader = make_reader(f, decoder_factories=[Ros1DecoderFactory(), ProtobufDecoderFactory()])
        summary = reader.get_summary()
        has_new_gps = "/point_one/pose" in [chan.topic for cid, chan in summary.channels.items()]
        return 2 if has_new_gps else 1


def determine_camera_types(filepath: str):
    with open(filepath, "rb") as f:
        reader = make_reader(f, decoder_factories=[Ros1DecoderFactory(), ProtobufDecoderFactory()])
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
            if chan.topic == "robot.h264_video.front" and has_new_front_cam is None:
                has_new_front_cam = is_new_cam(dmsg.metadata)
            elif chan.topic == "robot.h264_video.right" and has_new_right_cam is None:
                has_new_right_cam = is_new_cam(dmsg.metadata)
            elif chan.topic == "robot.h264_video.left" and has_new_left_cam is None:
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
        )


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
            if chan.topic == "robot.h264_video.front" and has_new_front_cam is None:
                has_new_front_cam = is_new_cam(dmsg.metadata)
            elif chan.topic == "robot.h264_video.right" and has_new_right_cam is None:
                has_new_right_cam = is_new_cam(dmsg.metadata)
            elif chan.topic == "robot.h264_video.left" and has_new_left_cam is None:
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


# TODO(Brad): remove after merge
def merge_mcaps(input_files, output_fp, keep=False):
    cmd = f'mcap merge {" ".join([str(p) for p in input_files])} -o {output_fp}'
    logged_cmd(cmd)
    if not keep:
        remove_files(input_files)


def trim_video_files(video_files, valid_start_s, valid_end_s, tmp_dir, keep=False, chunk_duration_s=60.0):
    if not video_files:
        return []

    trimmed_files = []
    for video_fp in sorted(video_files, key=parse_timestamp_from_mcap):
        file_start_s = parse_timestamp_from_mcap(video_fp)
        file_end_s = file_start_s + chunk_duration_s
        trim_start_s = max(valid_start_s, file_start_s)
        trim_end_s = min(valid_end_s, file_end_s)

        if trim_start_s >= trim_end_s:
            continue

        if trim_start_s == file_start_s and trim_end_s == file_end_s:
            trimmed_files.append(video_fp)
            continue

        output_fp = str(tmp_dir / f"{Path(video_fp).stem}_trimmed.mcap")
        cmd = f"mcap filter {video_fp} -s {int(trim_start_s)} -e {int(trim_end_s)} -o {output_fp}"
        logged_cmd(cmd)
        trimmed_files.append(output_fp)
        if not keep:
            remove_files([video_fp])

    return trimmed_files


class LogDownloadError(RuntimeError):
    pass


def download_logs(
    s3_client,
    log_files: list[str],
    output_dir: Path,
    bucket="coco-gg-bags-prod",
) -> list[str]:
    """Download the give log files"""
    local_files = []
    logger.info(f"Will download {len(log_files)} files")
    for key in log_files:
        output_fp = output_dir / key.rsplit("/", 1)[-1]
        local_files.append(str(output_fp))

        if output_fp.exists():
            logger.debug("skipping download; already exists %s", output_fp)
            continue

        s3_uri = f"s3://{bucket}/{key}"
        logger.debug("downloading %s -> %s", s3_uri, output_fp)

        try:
            s3_client.download_file(bucket, key, str(output_fp))
        except Exception as exc:
            raise LogDownloadError(f"failed to download {s3_uri} -> {output_fp}") from exc
    return local_files


def convert_bags_to_mcaps(files: list[str], keep_bags=False):
    """Give a list of files it will convert any .bag to .mcap"""
    bag_to_mcap = {}
    for input_fp in files:
        base_filepath, ext = os.path.splitext(input_fp)
        if ext != ".bag":
            continue
        logger.debug(f"converting {input_fp} to an mcap")

        output_fp = f"{base_filepath}.mcap"
        if os.path.exists(output_fp):
            logger.debug(f"mcap for bag: {input_fp} already exists. skipping ...")
            bag_to_mcap[input_fp] = output_fp
            continue

        # perform conversion using mcap binary
        logged_cmd(f"mcap convert {input_fp} {output_fp}", quiet=True)

        # cleanup
        if not keep_bags:
            logger.debug(f"removing {input_fp}")
            os.remove(input_fp)
        bag_to_mcap[input_fp] = output_fp
    return bag_to_mcap


@dataclass
class FileIndex:
    files: list[str]

    def replace(self, old_files, new_files):
        if isinstance(new_files, str):
            new_files = [new_files]
        if isinstance(old_files, str):
            old_files = [old_files]

        for old_file in old_files:
            self.files.remove(old_file)
        self.files += new_files

    def add(self, files):
        if isinstance(files, list):
            files = [str(f) for f in files]
        if isinstance(files, str):
            files = [files]
        if isinstance(files, Path):
            files = [str(files)]
        self.files += files


def cleanup(tmp_dir):
    for file in os.listdir(tmp_dir):
        fp = os.path.join(tmp_dir, file)
        logger.debug(f"Removing {fp}")
        os.remove(fp)
    logger.debug(f"Removing {tmp_dir}")
    os.rmdir(tmp_dir)


def remove_files(files):
    for file in files:
        logger.debug(f"Removing {file}")
        os.remove(file)


def compute_avg_speed(filepath: str, frequency_hz=1):
    with open(filepath, "rb") as f:
        reader = make_reader(f, decoder_factories=[Ros1DecoderFactory()])
        speeds = []
        last_t = float("-inf")
        period = 1.0 / max(float(frequency_hz), 1e-9)
        for _, _, msg, dmsg in reader.iter_decoded_messages(topics=["/speed_fb"]):
            t = msg.log_time / 1e9
            if t - last_t < period:
                continue
            last_t = t
            speeds.append(dmsg.car_speed)
        return np.array(speeds).mean()


class UnsuitableLogError(RuntimeError):
    pass


class NoValidDataError(UnsuitableLogError):
    pass


def s3_key_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def process_message(
    body: str,
    attributes: Dict[str, Any],
    base_tmp_dir: Path,
    keep: bool,
    debug: bool,
) -> None:
    """Replace this with your real work."""
    local_files = []
    s3 = build_s3_client()
    file_index = FileIndex([])

    payload = json.loads(body) if body.strip().startswith("{") else {"body": body}
    pilot_assignment_id = payload.get("pilot_assignment_id")
    trip_type = payload.get("trip_type")
    token = pilot_assignment_id_ctx.set(pilot_assignment_id)
    start_time = time.monotonic()
    try:
        logger.debug("using tmp_dir=%s", base_tmp_dir)
        logger.info(f"processing {pilot_assignment_id=} {trip_type=} {attributes=}")
        tmp_dir = base_tmp_dir / pilot_assignment_id
        tmp_dir.mkdir(exist_ok=True)

        output_bucket = "coco-trip-clips-976053906881-us-west-2"
        start_dt = datetime.fromtimestamp(int(payload["valid_start"]))
        output_key_prefix = f"v2/year={start_dt.year}/month={start_dt.month}/day={start_dt.day}"
        output_key = f"{output_key_prefix}/{pilot_assignment_id}.mcap"
        exists = s3_key_exists(s3, output_bucket, output_key)
        if exists:
            logger.info(f"s3://{output_bucket}/{output_key} exists will not process")
            return

        # 0. download video files and recover any bad mcaps before finding overlap
        log_files = payload["log_files"]
        video_keys = [f for f in log_files if f.endswith("_h264.mcap")]
        video_local_files = download_logs(s3, video_keys, tmp_dir)
        video_key_to_local = {key: str(tmp_dir / key.rsplit("/", 1)[-1]) for key in video_keys}
        ensure_video_mcaps_readable(video_local_files)

        # 1. find best overlap using mcap timing for videos
        interval, bag_files, video_files = find_best_overlap(
            log_files,
            use_mcap_timing=True,
            mcap_timing_func=get_mcap_timing,
            mcap_timing_path_map=video_key_to_local,
        )
        assert interval is not None, "No overlap in logs"
        if interval is None:
            NoValidDataError("No overlap b/t bags and videos")

        valid_start_s = interval.start
        valid_end_s = interval.end
        assert valid_start_s < valid_end_s, "End time is below start time"
        og_duration = int(payload["valid_end"]) - int(payload["valid_start"])
        overlap_s = int(valid_end_s - valid_start_s)
        logger.info(f"{overlap_s}s of overlap (lost {og_duration - overlap_s}s)")

        # 2. download bag logs for a given trip. if any fail we should fail the entire log
        local_bag_files = download_logs(s3, bag_files, tmp_dir)
        file_index.add(local_bag_files)
        video_local_overlap = [video_key_to_local[key] for key in video_files]
        file_index.add(video_local_overlap)

        # 3. convert any bags to mcap, unify them into one file, and then filter down to topics and time range we care about
        # ... convert
        bag_files = [f for f in local_bag_files if f.endswith(".bag")]
        logger.info(f"Will convert {len(bag_files)} bag files")
        bag_to_mcap = convert_bags_to_mcaps(bag_files, keep_bags=keep)
        bag_mcap_files = bag_to_mcap.values()
        file_index.replace(bag_to_mcap.keys(), bag_to_mcap.values())

        # ... merge them into single file
        logger.info(f"Merging {len(bag_mcap_files)} into a single one")
        unified_bag_fp = str(tmp_dir / "unifed_bags.mcap")
        merge_mcaps(bag_mcap_files, unified_bag_fp, keep=keep)
        file_index.replace(bag_mcap_files, unified_bag_fp)

        # directory now looks like this
        # <tmp>
        #   unifed_bags.mcap
        #   <prefix0>_h264.mcap
        #   ...
        #   <prefixN>_h264.mcap

        # 3) metadata computation
        # 3a)
        # We need to do some logic based on the gps trace. We want to:
        #   1) clip the log to START after we're X meters from start and END X meters before destination
        #   2) also compute an envelope to trim the route if we only have partial data for the trip
        should_mask_origin = payload["trip_type"] in ("DELIVERY_TRIP", "RETURN_TRIP")
        should_mask_dest = payload["trip_type"] in ("DELIVERY_TRIP", "RETURN_TRIP", "JITP_TRIP")
        logger.info(f"Determing valid start/end by analyzing trace. {should_mask_origin=} {should_mask_dest=}")
        origin_pt = wkb.loads(bytes.fromhex(payload["origin_point_hexwkb"])) if should_mask_origin else None
        destination_pt = wkb.loads(bytes.fromhex(payload["destination_point_hexwkb"])) if should_mask_dest else None
        geo_valid_start_s, geo_valid_end_s, trace_env = find_valid_start_end_from_trace(
            unified_bag_fp,
            origin_pt,
            destination_pt,
            start_ns=int(valid_start_s * 1e9),
            end_ns=int(valid_end_s * 1e9),
            frequency_hz=0.5,
        )
        if geo_valid_start_s is None:
            raise NoValidDataError("no valid start found based on trace")
        if geo_valid_end_s is None:
            raise NoValidDataError("no valid end found based on trace")
        if geo_valid_end_s < geo_valid_start_s:
            raise NoValidDataError("route based filter has end before start. This means there is probably no valid point")
        valid_start_s = max(geo_valid_start_s, valid_start_s)
        valid_end_s = min(geo_valid_end_s, valid_end_s)
        if valid_start_s > valid_end_s:
            raise NoValidDataError("End time is below start time")

        if debug:
            geo_debug_fp = tmp_dir / "geo_debug.mcap"
            write_out_geo_debug(geo_debug_fp, valid_start_s, origin_pt, destination_pt, trace_env)
            file_index.add(geo_debug_fp)

        # compute some additional metadata (will need for injecting calibration)
        aux_vehicle_metadata = {}
        gps_version = determine_gps_type(unified_bag_fp)
        aux_vehicle_metadata["gps_version"] = gps_version
        video_files = [f for f in file_index.files if f.endswith("h264.mcap")]
        assert len(video_files) > 0, f"no video files found:  {file_index.files}"
        cam_metadata = determine_camera_types(video_files[0])
        aux_vehicle_metadata.update(cam_metadata)
        logger.info(f"Computed new metadata {aux_vehicle_metadata=}")

        # 4. inject any additional data (e.g. routes and map issues)
        # ... map issues
        map_issues = payload.get("map_issues")
        map_issues = [] if map_issues is None else map_issues
        logger.info(f"Adding {len(map_issues)} map issues to the log")
        map_issue_output = tmp_dir / "map_issues.mcap"
        write_out_map_issues(map_issue_output, map_issues)
        file_index.add(map_issue_output)

        # ... inject camera calibration (and tfs?)
        calib_output = tmp_dir / "calibration.mcap"
        write_out_camera_calibration(calib_output, aux_vehicle_metadata["camera_version"], valid_start_s)
        file_index.add(calib_output)

        # ... routes
        route_output = tmp_dir / "routes.mcap"
        logger.info(f"Adding {payload['routes']} to the log")
        write_out_routes(route_output, payload["routes"], valid_start_s, origin_pt, destination_pt, envelope=trace_env)
        file_index.add(route_output)

        non_video_files = [f for f in file_index.files if not f.endswith("h264.mcap")]
        logger.info("Building merged non-video file")
        merged_non_video_fp = str(tmp_dir / "merged_non_video.mcap")
        merge_mcaps(non_video_files, merged_non_video_fp, keep=keep)

        logger.info("Filtering merged non-video file")
        filtered_non_video_fp = str(tmp_dir / "filtered_non_video.mcap")
        include_str = "".join([f' -y "{t}"   ' for t in TOPICS])
        cmd = f"mcap filter {merged_non_video_fp} {include_str} -s {int(valid_start_s)} -e {int(valid_end_s)} -o {filtered_non_video_fp}"
        logged_cmd(cmd)
        if not keep:
            remove_files([merged_non_video_fp])
        file_index.replace(non_video_files, filtered_non_video_fp)

        video_files = [f for f in file_index.files if f.endswith("h264.mcap")]
        trimmed_video_files = trim_video_files(video_files, valid_start_s, valid_end_s, tmp_dir, keep=keep)
        if video_files:
            file_index.replace(video_files, trimmed_video_files)
        if video_files and not trimmed_video_files:
            logger.warning("No video files intersected the valid range after trimming")

        # Combine data into single mcap for the assignment
        # ... merge
        logger.info("Building final merged file")
        filtered_output_fp = str(tmp_dir / "final.mcap")
        merge_mcaps(file_index.files, filtered_output_fp, keep=keep)

        # 5. infer additional metadata
        # ... check for existance of /point_one/pose
        # ... check resolution of left/front/right cameras
        existing_metadata = payload.get("external_metadata")
        existing_metadata["__version"] = 2
        start_s, end_s, duration_s = get_mcap_timing(filtered_output_fp)
        existing_metadata["clip_start_utc"] = datetime.fromtimestamp(start_s).isoformat() + "Z"
        existing_metadata["clip_end_utc"] = datetime.fromtimestamp(end_s).isoformat() + "Z"
        existing_metadata["clip_duration_seconds"] = duration_s
        existing_metadata["vehicle"].update(aux_vehicle_metadata)
        existing_metadata["clip_avg_speed"] = round(compute_avg_speed(filtered_output_fp), 2)
        logger.info("parsed metadata=%s", json.dumps(existing_metadata))

        # 6. quality check
        # ... check that all topics are there
        # TODO(Brad): do this

        # 7. upload
        flattened_metadata = flatten(existing_metadata)
        logger.info(f"Uploading to s3://{output_bucket}/{output_key} with metadata: {flattened_metadata}")
        s3.upload_file(filtered_output_fp, output_bucket, output_key, ExtraArgs={"Metadata": flattened_metadata})

        # TODO(Brad): do this
        if debug:
            shutil.move(filtered_output_fp, base_tmp_dir / "final.mcap")

    except NoValidDataError as e:
        logger.warning(f"No valid data found. Will place marker in s3 and will consume message from queue: {e}")
        invalid_metadata = {"__version": "2", "error_code": "NO_VALID_DATA"}
        empty_fp = Path(f"{tmp_dir}/empty.mcap")
        empty_fp.touch()
        s3.upload_file(empty_fp, output_bucket, output_key, ExtraArgs={"Metadata": invalid_metadata})
        logger.info(f"Processed {pilot_assignment_id}")
    else:
        duration_s = time.monotonic() - start_time
        logger.info(f"Processed {pilot_assignment_id} in {duration_s:.2f}s")
    finally:
        if not keep:
            cleanup(tmp_dir)
        pilot_assignment_id_ctx.reset(token)


def flatten(d, sep="__", prefix=""):
    out = {}
    for k, v in d.items():
        key = f"{prefix}{sep}{k}" if prefix else k
        out.update(flatten(v, sep, key) if isinstance(v, dict) else {key: str(v)})
    return out


def poll_loop(queue_url: str, *, max_messages: int, wait_time: int, visibility_timeout: int, once: bool, tmp_dir: Path, keep: bool, debug: bool) -> None:
    sqs = build_sqs_client()
    while True:
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
                process_message(
                    msg.get("Body", ""),
                    msg.get("MessageAttributes", {}),
                    tmp_dir,
                    keep=keep,
                    debug=debug,
                )
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
            except Exception:
                logger.exception("message processing failed; leaving in queue")

        if once:
            return


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SQS worker skeleton")
    parser.add_argument(
        "--queue-url",
        default=os.getenv("QUEUE_URL"),
        help="SQS queue URL (or set QUEUE_URL)",
    )
    parser.add_argument("--max-messages", type=int, default=1, help="Messages per poll")
    parser.add_argument("--wait-time", type=int, default=10, help="Long poll wait time (seconds)")
    parser.add_argument("--visibility-timeout", type=int, default=3600, help="Visibility timeout (seconds)")
    parser.add_argument("--once", action="store_true", help="Process a single poll and exit")
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--tmp-dir", help="Override temp directory path")
    parser.add_argument(
        "--persist-tmp",
        action="store_true",
        help="Use a persistent temp directory under ./.tmp/data_archiver",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    pilot_filter = PilotAssignmentFilter()
    if args.debug:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s pilot_assignment_id=%(pilot_assignment_id)s: %(message)s"))
        handler.addFilter(pilot_filter)
        logging.basicConfig(handlers=[handler])
        logger.setLevel(log_level)
    else:
        handler = logging.StreamHandler()
        handler.setFormatter(LevelJsonFormatter())
        handler.addFilter(pilot_filter)
        logging.basicConfig(handlers=[handler])
        logger.setLevel(log_level)
    if not args.queue_url:
        raise SystemExit("queue URL is required via --queue-url or QUEUE_URL")
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
        keep=args.keep,
        debug=args.debug,
    )
    return 0


if __name__ == "__main__":
    main()
