"""Simple SQS worker skeleton."""

from __future__ import annotations

import argparse
from contextvars import ContextVar
from collections.abc import Iterable
from dataclasses import dataclass, field
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


def mcap_merge(input_files, output_fp, keep=False):
    cmd = f'mcap merge {" ".join([str(p) for p in input_files])} -o {output_fp}'
    logged_cmd(cmd)
    if not keep:
        remove_files(input_files)


def mcap_filter(input_fp: str, output_fp: str, *, include: list[str], start_s: int, end_s: int) -> None:
    include_str = "".join([f' -y "{t}"   ' for t in include])
    cmd = f"mcap filter {input_fp} {include_str} -s {int(start_s)} -e {int(end_s)} -o {output_fp}"
    logged_cmd(cmd)


def mcap_convert(input_fp: str, output_fp: str) -> None:
    logged_cmd(f"mcap convert {input_fp} {output_fp}", quiet=True)


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


def _infer_camera_metadata(reader) -> tuple[str, str]:
    has_new_front_cam = None
    has_new_left_cam = None
    has_new_right_cam = None
    vehicle_model = "1"
    camera_version = "1"

    def is_new_cam(metadata):
        for m in metadata:
            if m.key == "codedWidth":
                return int(m.value) == 1920
        return False

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

    return vehicle_model, camera_version


def determine_camera_types(filepath: str) -> dict[str, str]:
    with open(filepath, "rb") as f:
        reader = make_reader(f, decoder_factories=[Ros1DecoderFactory(), ProtobufDecoderFactory()])
        vehicle_model, camera_version = _infer_camera_metadata(reader)
        return dict(
            vehicle_model=vehicle_model,
            camera_version=camera_version,
        )


def infer_additional_vehicle_metadata(filepath: str) -> dict[str, str]:
    with open(filepath, "rb") as f:
        reader = make_reader(f, decoder_factories=[Ros1DecoderFactory(), ProtobufDecoderFactory()])
        summary = reader.get_summary()
        has_new_gps = "/point_one/pose" in [chan.topic for cid, chan in summary.channels.items()]
        vehicle_model, camera_version = _infer_camera_metadata(reader)
        return dict(
            vehicle_model=vehicle_model,
            camera_version=camera_version,
            gps_version="2" if has_new_gps else "1",
        )


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
        mcap_filter(video_fp, output_fp, include=[], start_s=trim_start_s, end_s=trim_end_s)
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
        mcap_convert(input_fp, output_fp)

        # cleanup
        if not keep_bags:
            logger.debug(f"removing {input_fp}")
            os.remove(input_fp)
        bag_to_mcap[input_fp] = output_fp
    return bag_to_mcap


@dataclass
class LogArtifacts:
    tmp_dir: Path
    keep: bool
    files: list[str] = field(default_factory=list)

    def add(self, files):
        self.files.extend(normalize_files(files))

    def replace(self, old_files, new_files):
        self.files = replace_files(self.files, old_files, new_files)

    def cleanup(self):
        if self.keep:
            return
        cleanup(self.tmp_dir)


def normalize_files(files) -> list[str]:
    if isinstance(files, (list, tuple, set)):
        return [str(f) for f in files]
    if isinstance(files, str):
        return [files]
    if isinstance(files, Path):
        return [str(files)]
    if isinstance(files, Iterable):
        return [str(f) for f in files]
    return []


def replace_files(files: list[str], old_files, new_files) -> list[str]:
    old_list = normalize_files(old_files)
    new_list = normalize_files(new_files)
    remaining = [f for f in files if f not in old_list]
    return remaining + new_list


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


@dataclass(frozen=True)
class WorkerConfig:
    output_bucket: str
    output_key_prefix: str
    output_key: str


def build_worker_config(payload: dict) -> WorkerConfig:
    output_bucket = "coco-trip-clips-976053906881-us-west-2"
    start_dt = datetime.fromtimestamp(int(payload["valid_start"]))
    output_key_prefix = f"v3/year={start_dt.year}/month={start_dt.month}/day={start_dt.day}"
    output_key = f"{output_key_prefix}/{payload['pilot_assignment_id']}.mcap"
    return WorkerConfig(
        output_bucket=output_bucket,
        output_key_prefix=output_key_prefix,
        output_key=output_key,
    )


def build_metadata(payload: dict, filtered_output_fp: str, aux_vehicle_metadata: dict) -> dict:
    existing_metadata = payload.get("external_metadata")
    existing_metadata["__version"] = 2
    start_s, end_s, duration_s = get_mcap_timing(filtered_output_fp)
    existing_metadata["clip_start_utc"] = datetime.fromtimestamp(start_s).isoformat() + "Z"
    existing_metadata["clip_end_utc"] = datetime.fromtimestamp(end_s).isoformat() + "Z"
    existing_metadata["clip_duration_seconds"] = duration_s
    existing_metadata["vehicle"].update(aux_vehicle_metadata)
    existing_metadata["clip_avg_speed"] = round(compute_avg_speed(filtered_output_fp), 2)
    return existing_metadata


def fetch_and_prepare_logs(
    s3,
    log_files: list[str],
    tmp_dir: Path,
    keep: bool,
) -> list[str]:
    local_files = download_logs(s3, log_files, tmp_dir)

    # ... bags
    bag_files = [f for f in local_files if f.endswith(".bag")]
    if bag_files:
        logger.info(f"Will convert {len(bag_files)} bag files")
    bag_to_mcap = convert_bags_to_mcaps(bag_files, keep_bags=keep)
    mcap_files = replace_files(local_files, bag_to_mcap.keys(), bag_to_mcap.values())

    # ... mcaps
    video_files = [f for f in mcap_files if f.endswith("_h264.mcap")]
    ensure_video_mcaps_readable(video_files)
    return mcap_files


def build_unified_bag(bag_mcap_files: list[str], tmp_dir: Path, keep: bool, artifacts: LogArtifacts) -> str:
    artifacts.add(bag_mcap_files)

    # 1) merge them all into a single bag
    logger.info(f"Merging {len(bag_mcap_files)} into a single one")
    unified_bag_fp = str(tmp_dir / "unifed_bags.mcap")
    mcap_merge(bag_mcap_files, unified_bag_fp, keep=keep)

    # 2) filter for only topics we are about
    logger.info("Filter bag file to remove unused topics")
    unified_bag_filtered_fp = str(tmp_dir / "unifed_bags_filtered.mcap")
    include = [t for t in TOPICS if "camera_info" not in t and "tf_static" not in t]
    include_str = "".join([f' -y "{t}"   ' for t in include])
    cmd = f"mcap filter {unified_bag_fp} {include_str} -o {unified_bag_filtered_fp}"
    logged_cmd(cmd)
    if not keep:
        remove_files([unified_bag_fp])
    artifacts.replace(bag_mcap_files, unified_bag_filtered_fp)
    return unified_bag_filtered_fp


def apply_trace_filters(
    unified_bag_fp: str,
    payload: dict,
    valid_start_s: float,
    valid_end_s: float,
    tmp_dir: Path,
    artifacts: LogArtifacts,
    debug: bool,
) -> tuple[float, float, Any, Any, Any]:
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
        artifacts.add(geo_debug_fp)

    return valid_start_s, valid_end_s, trace_env, origin_pt, destination_pt


def inject_enrichments(
    payload: dict,
    valid_start_s: float,
    aux_vehicle_metadata: dict,
    origin_pt: Any,
    destination_pt: Any,
    trace_env: Any,
    tmp_dir: Path,
    artifacts: LogArtifacts,
) -> None:
    map_issues = payload.get("map_issues")
    map_issues = [] if map_issues is None else map_issues
    logger.info(f"Adding {len(map_issues)} map issues to the log")
    map_issue_output = tmp_dir / "map_issues.mcap"
    write_out_map_issues(map_issue_output, map_issues)
    artifacts.add(map_issue_output)

    calib_output = tmp_dir / "calibration.mcap"
    write_out_camera_calibration(calib_output, aux_vehicle_metadata["camera_version"], valid_start_s)
    artifacts.add(calib_output)

    route_output = tmp_dir / "routes.mcap"
    logger.info(f"Adding {payload['routes']} to the log")
    write_out_routes(route_output, payload["routes"], valid_start_s, origin_pt, destination_pt, envelope=trace_env)
    artifacts.add(route_output)


def build_non_video_file(
    artifacts: LogArtifacts,
    valid_start_s: float,
    valid_end_s: float,
    tmp_dir: Path,
    keep: bool,
) -> str:
    non_video_files = [f for f in artifacts.files if not f.endswith("h264.mcap")]
    logger.info("Building merged non-video file")
    merged_non_video_fp = str(tmp_dir / "merged_non_video.mcap")
    mcap_merge(non_video_files, merged_non_video_fp, keep=keep)

    logger.info("Filtering merged non-video file")
    filtered_non_video_fp = str(tmp_dir / "filtered_non_video.mcap")
    mcap_filter(merged_non_video_fp, filtered_non_video_fp, include=TOPICS, start_s=valid_start_s, end_s=valid_end_s)
    if not keep:
        remove_files([merged_non_video_fp])
    artifacts.replace(non_video_files, filtered_non_video_fp)
    return filtered_non_video_fp


def trim_and_replace_videos(
    artifacts: LogArtifacts,
    valid_start_s: float,
    valid_end_s: float,
    tmp_dir: Path,
    keep: bool,
) -> None:
    video_files = [f for f in artifacts.files if f.endswith("h264.mcap")]
    trimmed_video_files = trim_video_files(video_files, valid_start_s, valid_end_s, tmp_dir, keep=keep)
    if video_files:
        artifacts.replace(video_files, trimmed_video_files)
    if video_files and not trimmed_video_files:
        logger.warning("No video files intersected the valid range after trimming")


def merge_final_output(artifacts: LogArtifacts, tmp_dir: Path, keep: bool) -> str:
    logger.info("Building final merged file")
    filtered_output_fp = str(tmp_dir / "final.mcap")
    mcap_merge(artifacts.files, filtered_output_fp, keep=keep)
    return filtered_output_fp


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


def process_message(
    body: str,
    attributes: Dict[str, Any],
    base_tmp_dir: Path,
    keep: bool,
    debug: bool,
) -> None:
    """Replace this with your real work."""
    s3 = build_s3_client()
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
        artifacts = LogArtifacts(tmp_dir=tmp_dir, keep=keep)

        config = build_worker_config(payload)
        exists = s3_key_exists(s3, config.output_bucket, config.output_key)
        if exists:
            logger.info(f"s3://{config.output_bucket}/{config.output_key} exists will not process")
            # return

        # 0. download logs and convert any bags to mcaps before finding overlap
        log_files = payload["log_files"]
        mcap_files = fetch_and_prepare_logs(s3, log_files, tmp_dir, keep)
        artifacts.add(mcap_files)

        # 1. find best overlap using mcap timing for all files
        interval, bag_files, video_files = find_best_overlap(mcap_files, use_mcap_timing=True, mcap_timing_func=get_mcap_timing)
        assert interval is not None, "No overlap in logs"
        if interval is None:
            NoValidDataError("No overlap b/t bags and videos")

        valid_start_s, valid_end_s = interval.start, interval.end
        assert valid_start_s < valid_end_s, "End time is below start time"
        og_duration = int(payload["valid_end"]) - int(payload["valid_start"])
        overlap_s = int(valid_end_s - valid_start_s)
        logger.info(f"{overlap_s}s of overlap (lost {og_duration - overlap_s}s)")

        # 2. Merge all bags into single one and filter out topics we don't want
        unified_bag_fp = build_unified_bag(bag_files, tmp_dir, keep, artifacts)

        # 3. convert any bags to mcap, unify them into one file, and then filter down to topics and time range we care about
        # 3) metadata computation
        # 3a)
        # We need to do some logic based on the gps trace. We want to:
        #   1) clip the log to START after we're X meters from start and END X meters before destination
        #   2) also compute an envelope to trim the route if we only have partial data for the trip
        valid_start_s, valid_end_s, trace_env, origin_pt, destination_pt = apply_trace_filters(
            unified_bag_fp,
            payload,
            valid_start_s,
            valid_end_s,
            tmp_dir,
            artifacts,
            debug,
        )

        # compute some additional metadata (will need for injecting calibration)
        aux_vehicle_metadata = {}
        gps_version = determine_gps_type(unified_bag_fp)
        aux_vehicle_metadata["gps_version"] = gps_version
        video_files = [f for f in artifacts.files if f.endswith("h264.mcap")]
        assert len(video_files) > 0, f"no video files found:  {artifacts.files}"
        cam_metadata = determine_camera_types(video_files[0])
        aux_vehicle_metadata.update(cam_metadata)
        logger.info(f"Computed new metadata {aux_vehicle_metadata=}")

        # 4. inject any additional data (e.g. routes and map issues)
        inject_enrichments(payload, valid_start_s, aux_vehicle_metadata, origin_pt, destination_pt, trace_env, tmp_dir, artifacts)
        build_non_video_file(artifacts, valid_start_s, valid_end_s, tmp_dir, keep)
        trim_and_replace_videos(artifacts, valid_start_s, valid_end_s, tmp_dir, keep)

        # Combine data into single mcap for the assignment
        # ... merge
        filtered_output_fp = merge_final_output(artifacts, tmp_dir, keep)
        logger.info("Filtering final merged file")
        final_filtered_output_fp = str(tmp_dir / "final_filtered.mcap")
        mcap_filter(filtered_output_fp, final_filtered_output_fp, include=TOPICS, start_s=valid_start_s, end_s=valid_end_s)
        if not keep:
            remove_files([filtered_output_fp])
        filtered_output_fp = final_filtered_output_fp

        # 5. infer additional metadata
        # ... check for existance of /point_one/pose
        # ... check resolution of left/front/right cameras
        existing_metadata = build_metadata(payload, filtered_output_fp, aux_vehicle_metadata)
        logger.info("parsed metadata=%s", json.dumps(existing_metadata))

        # 6. quality check
        ok, error = quality_check(filtered_output_fp)
        if not ok:
            logger.warning(f"Failed quality check: {error}")
            raise NoValidDataError(f"Failed quality check: {error}")
        # ... check that all topics are there
        # TODO(Brad): do this

        # 7. upload
        flattened_metadata = flatten(existing_metadata)
        logger.info(f"Uploading to s3://{config.output_bucket}/{config.output_key} with metadata: {flattened_metadata}")
        s3.upload_file(filtered_output_fp, config.output_bucket, config.output_key, ExtraArgs={"Metadata": flattened_metadata})

        # TODO(Brad): do this
        if debug:
            shutil.move(filtered_output_fp, base_tmp_dir / "final.mcap")

    except NoValidDataError as e:
        logger.warning(f"No valid data found. Will place marker in s3 and will consume message from queue: {e}")
        invalid_metadata = {"__version": "2", "error_code": "NO_VALID_DATA"}
        empty_fp = Path(f"{tmp_dir}/empty.mcap")
        empty_fp.touch()
        s3.upload_file(empty_fp, config.output_bucket, config.output_key, ExtraArgs={"Metadata": invalid_metadata})
        logger.info(f"Processed {pilot_assignment_id}")
    else:
        duration_s = time.monotonic() - start_time
        logger.info(f"Processed {pilot_assignment_id} in {duration_s:.2f}s")
    finally:
        artifacts.cleanup()
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
                process_message(msg.get("Body", ""), msg.get("MessageAttributes", {}), tmp_dir, keep=keep, debug=debug)
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
    if args.debug:
        TOPICS.append("/debug/pts")
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
