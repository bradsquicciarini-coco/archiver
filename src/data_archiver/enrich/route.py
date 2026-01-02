import base64
from datetime import datetime
import json
import math
from typing import Optional, Tuple
import foxglove
from shapely import LineString, wkb
from shapely.geometry import mapping
from foxglove.schemas import GeoJson
from foxglove.channels import GeoJsonChannel

from shapely.geometry import Point
from shapely.ops import transform
from pyproj import CRS, Transformer

from mcap_ros1.decoder import DecoderFactory as Ros1DecoderFactory
from mcap_protobuf.decoder import DecoderFactory as ProtobufDecoderFactory
from mcap.reader import make_reader


def geodesic_circle_geojson(lon: float, lat: float, radius_m: float, steps: int = 128):
    # Local azimuthal equidistant projection centered on the point
    aeqd = CRS.from_proj4(f"+proj=aeqd +lat_0={lat} +lon_0={lon} +datum=WGS84 +units=m +no_defs")
    wgs84 = CRS.from_epsg(4326)

    fwd = Transformer.from_crs(wgs84, aeqd, always_xy=True).transform
    inv = Transformer.from_crs(aeqd, wgs84, always_xy=True).transform

    center_ll = Point(lon, lat)
    center_xy = transform(fwd, center_ll)

    circle_xy = center_xy.buffer(radius_m, resolution=max(4, steps // 4))
    circle_ll = transform(inv, circle_xy)

    return {"type": "Feature", "geometry": mapping(circle_ll), "properties": {"radius_m": radius_m, "center": [lon, lat]}}


def write_out_routes(output_fp: str, routes, min_epoch_ts, origin_pt, dest_pt, envelope=None, topic="/route/geojson"):
    channel = GeoJsonChannel(topic=topic)

    start_idx = 0
    for i, r in enumerate(routes):
        if r["active_start"] < min_epoch_ts:
            start_idx = i

    with foxglove.open_mcap(output_fp, allow_overwrite=True):
        for r in routes[start_idx:]:
            start_ts = max(r["active_start"], min_epoch_ts)
            geom = wkb.loads(base64.b64decode(r["geom_b64"]))
            clipped_route = clip_route_keep_last_v2(geom, origin_pt, dest_pt)
            if clipped_route is None:
                print(f'route is empty {r["id"]=}')
                continue

            if envelope:
                clipped_route = clip_route_keep_last(clipped_route, envelope)
            if clipped_route is None:
                print(f'route is empty {r["id"]=}')
                continue

            geojson = {
                "type": "Feature",
                "geometry": mapping(clipped_route),
                "properties": {
                    "route_id": r["id"],
                    "created_at": datetime.fromtimestamp(start_ts).isoformat() + "Z",
                },
            }
            route_pb = GeoJson(geojson=json.dumps(geojson))
            channel.log(route_pb, log_time=int(start_ts * 1e9))


def write_out_geo_debug(output_fp: str, min_epoch_ts, origin_pt, dest_pt, envelope=None):
    # debug
    channel_debug = GeoJsonChannel(topic="/debug/pts")
    with foxglove.open_mcap(output_fp, allow_overwrite=True):
        for pt in [origin_pt, dest_pt]:
            if pt is None:
                continue
            circle = geodesic_circle_geojson(pt.xy[0][0], pt.xy[1][0], radius_m=50)
            circle_pb = GeoJson(geojson=json.dumps(circle))
            channel_debug.log(circle_pb, log_time=int(min_epoch_ts * 1e9))

        if envelope is not None:
            env_geojson = {"type": "Feature", "geometry": mapping(envelope)}
            env_pb = GeoJson(geojson=json.dumps(env_geojson))
            channel_debug.log(env_pb, log_time=int(min_epoch_ts * 1e9))


# TODO(Brad): need to "anonymize ths". Probably should be something like
# - build a radius of 50m around stay away points
# -   route -> find the last point on the route 50m from the beginning, clip it there
# -   trace -> find first timestamp where we leave the 50m and first timestamp where we enter the dropoff
#
# find first timestamp where we leave the 50m and first timestamp where we enter the dropoff
#  - use this as the bounds
# for the route, we should walk the route, find the first point that enters the bounds and the last one that leaves it


def haversine_m(lat1, lon1, lat2, lon2):
    r = 6371.0088 * 1000  # mean Earth radius in m
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def find_valid_start_end_from_trace(
    filepath: str,
    stay_away_origin=None,
    stay_away_dest=None,
    frequency_hz: float = 1.0,
    radius_m: float = 50.0,
    buffer_m: float = 50.0,
    start_ns=None,
    end_ns=None,
) -> Tuple[Optional[float], Optional[float], Optional[any]]:
    period = 1.0 / max(float(frequency_hz), 1e-9)
    pts = []
    start_ts = end_ts = None
    last_t = float("-inf")
    buffer_deg = buffer_m / 111_320.0

    def far_enough(lat, lon, p) -> bool:
        if p is None:
            return True
        # p is a shapely Point: x=lon, y=lat
        return haversine_m(lat, lon, p.y, p.x) > radius_m

    with open(filepath, "rb") as f:
        reader = make_reader(f, decoder_factories=[Ros1DecoderFactory(), ProtobufDecoderFactory()])
        for _, _, msg, dmsg in reader.iter_decoded_messages(topics=["/acu_driver/gps_nav_topic"], start_time=start_ns, end_time=end_ns):
            t = msg.log_time / 1e9
            if t - last_t < period:
                continue
            last_t = t

            lon, lat = dmsg.longitude, dmsg.latitude
            include = False

            if start_ts is None:
                if not far_enough(lat, lon, stay_away_origin):
                    continue
                start_ts = t
                include = True

            if far_enough(lat, lon, stay_away_dest):
                end_ts = t
                include = True

            if include:
                pts.append((lon, lat))

    if start_ts is None or len(pts) < 2:
        return None, None, None

    trace = LineString(pts).buffer(buffer_deg)

    return start_ts, end_ts, trace


def clip_route_keep_last_v2(route_geom, start_pt=None, end_pt=None, radius_m=50):
    """
    route_geom: LineString (lon, lat)
    start_pt/end_pt: shapely Point (lon, lat) or None
    """
    centers = [(p.x, p.y) for p in (start_pt, end_pt) if p is not None]  # (lng, lat)
    if not centers:
        return route_geom  # nothing to clip against

    def min_dist_m(lng, lat):
        return min(haversine_m(lat, lng, cy, cx) for cx, cy in centers)

    def inside_center(lng, lat):
        # Return (center_lng, center_lat) of the first center we're inside, else None
        for cx, cy in centers:
            if haversine_m(lat, lng, cy, cx) < radius_m:
                return cx, cy
        return None

    def boundary_point(p_in, p_out, center, iters=24):
        cx, cy = center
        a, b = p_in, p_out
        for _ in range(iters):
            mid = ((a[0] + b[0]) * 0.5, (a[1] + b[1]) * 0.5)
            if haversine_m(mid[1], mid[0], cy, cx) < radius_m:
                a = mid
            else:
                b = mid
        return b  # just outside

    all_pts = list(route_geom.coords)

    # Find first/last vertex outside ALL exclusion circles (min_dist >= radius)
    start_idx = end_idx = None
    for i, (lng, lat) in enumerate(all_pts):
        if min_dist_m(lng, lat) >= radius_m:
            start_idx = i if start_idx is None else start_idx
            end_idx = i

    if start_idx is None:
        return None

    coarse = all_pts[start_idx : end_idx + 1]

    # Snap start if previous point was inside any exclusion circle
    first = coarse[0]
    if start_idx > 0:
        prev = all_pts[start_idx - 1]
        center = inside_center(prev[0], prev[1])
        if center:
            first = boundary_point(prev, first, center)

    # Snap end if next point is inside any exclusion circle
    last = coarse[-1]
    if end_idx < len(all_pts) - 1:
        nxt = all_pts[end_idx + 1]
        center = inside_center(nxt[0], nxt[1])
        if center:
            last = boundary_point(nxt, last, center)

    pts = [first] + coarse[1:-1] + [last]
    return LineString(pts)


def clip_route_keep_last(route_geom, env):
    """
    route_geom: LineString
    env: envelope polygon (from geometry.envelope)
    """
    start_idx, end_idx = None, None
    for i, (x, y) in enumerate(route_geom.coords):
        p = Point(x, y)
        if env.contains(p) or env.touches(p):
            if start_idx is None:
                start_idx = i
            end_idx = i

    if start_idx is None or end_idx is None:
        return None

    all_pts = list(route_geom.coords)
    coarse_pts = all_pts[start_idx : end_idx + 1]

    def _segment_env_intersection(p0, p1):
        seg = LineString([p0, p1])
        inter = seg.intersection(env.boundary)
        if inter.is_empty:
            return None
        if inter.geom_type == "Point":
            return (inter.x, inter.y)
        if inter.geom_type == "MultiPoint":
            # pick the point closest to p0
            pts = list(inter.geoms)
            pts.sort(key=lambda g: Point(p0).distance(g))
            return (pts[0].x, pts[0].y)
        if inter.geom_type == "LineString":
            # segment overlaps boundary; pick endpoint closer to p0
            pts = [Point(inter.coords[0]), Point(inter.coords[-1])]
            pts.sort(key=lambda g: Point(p0).distance(g))
            return (pts[0].x, pts[0].y)
        return None

    first_pt = coarse_pts[0]
    if start_idx > 0:
        prev_pt = all_pts[start_idx - 1]
        hit = _segment_env_intersection(prev_pt, first_pt)
        if hit:
            first_pt = hit

    last_pt = coarse_pts[-1]
    if end_idx < len(all_pts) - 1:
        next_pt = all_pts[end_idx + 1]
        hit = _segment_env_intersection(last_pt, next_pt)
        if hit:
            last_pt = hit

    fine_pts = [first_pt] + coarse_pts[1:-1] + [last_pt]
    return LineString(fine_pts)
