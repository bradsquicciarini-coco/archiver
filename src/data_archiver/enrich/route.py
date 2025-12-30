import base64
from datetime import datetime
import json
import foxglove
from shapely import wkb
from shapely.geometry import mapping
from foxglove.schemas import GeoJson
from foxglove.channels import GeoJsonChannel

from shapely.geometry import Point
from shapely.ops import transform
from pyproj import CRS, Transformer


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


def write_out_routes(output_fp: str, routes, min_epoch_ts, origin_pt, dest_pt, topic="/route/geojson"):
    channel = GeoJsonChannel(topic=topic)
    channel_debug = GeoJsonChannel(topic="/debug/pts")
    with foxglove.open_mcap(output_fp, allow_overwrite=True):
        for r in routes:
            start_ts = max(r["active_start"], min_epoch_ts)
            geom = wkb.loads(base64.b64decode(r["geom_b64"]))
            geojson = {
                "type": "Feature",
                "geometry": mapping(geom),
                "properties": {
                    "route_id": r["id"],
                    "created_at": datetime.fromtimestamp(start_ts).isoformat() + "Z",
                },
            }
            route_pb = GeoJson(geojson=json.dumps(geojson))
            channel.log(route_pb, log_time=int(start_ts * 1e9))

        # debug
        for pt in [origin_pt, dest_pt]:
            circle = geodesic_circle_geojson(pt.xy[0][0], pt.xy[1][0], radius_m=50)
            circle_pb = GeoJson(geojson=json.dumps(circle))
            channel_debug.log(circle_pb, log_time=int(min_epoch_ts * 1e9))


# TODO(Brad): need to "anonymize ths". Probably should be something like
# - build a radius of 50m around stay away points
# -   route -> find the last point on the route 50m from the beginning, clip it there
# -   trace -> find first timestamp where we leave the 50m and first timestamp where we enter the dropoff
