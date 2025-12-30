import re
from google.protobuf.timestamp_pb2 import Timestamp
from coco import MapReport_pb2
from shapely import wkb

from google.protobuf import descriptor_pb2

import foxglove
from foxglove import Channel, Schema


def camel_to_snake(name: str) -> str:
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    return s2.lower()


def parse_map_issues(map_issues):
    """Convert map issues into protobufs (?)"""
    output = []
    for mi in map_issues:
        geom = wkb.loads(bytes.fromhex(mi["reported_location_hexwkb"]))
        lng = geom.xy[0][0]
        lat = geom.xy[1][0]
        issue_name_attr = "ISSUE_" + camel_to_snake(mi["issue_type"]).upper()
        report = MapReport_pb2.MapReport(
            id=mi["id"],
            type=getattr(MapReport_pb2, issue_name_attr),
            notes=mi["notes"],
            reported_location=MapReport_pb2.GeoPoint(lat=lat, lng=lng),
            created_at=Timestamp(seconds=int(mi["created_at"])),
        )

        output.append(report)
    return output


def build_fds(root_desc) -> descriptor_pb2.FileDescriptorSet:
    fds = descriptor_pb2.FileDescriptorSet()
    seen = set()

    def add_file(fd):
        if fd.name in seen:
            return
        seen.add(fd.name)
        for dep in fd.dependencies:
            add_file(dep)
        fd.CopyToProto(fds.file.add())

    add_file(root_desc)
    return fds


def write_out_map_issues(output_fp: str, map_issues, topic="/route/issue_report"):
    map_issues_pb = parse_map_issues(map_issues)
    # Build a protobuf schema from your generated module
    proto_fds = build_fds(MapReport_pb2.DESCRIPTOR)
    report_descriptor = MapReport_pb2.MapReport.DESCRIPTOR

    map_report_channel = Channel(
        topic=topic,
        message_encoding="protobuf",
        schema=Schema(
            name=f"{report_descriptor.file.package}.{report_descriptor.name}",
            encoding="protobuf",
            data=proto_fds.SerializeToString(),
        ),
    )
    with foxglove.open_mcap(output_fp, allow_overwrite=True):
        for report in map_issues_pb:
            map_report_channel.log(report.SerializeToString(), log_time=int(report.created_at.ToSeconds() * 1e9))
