install spatial
;
load spatial
;

pragma memory_limit = '14GB'
;
pragma temp_directory = '/tmp/duckdb_spill'
;
copy (
with
routes as (
    select
        a.pilot_assignment_id,
        list(
            struct_pack(
                id := r.internal_routing_id,
                geom_b64 := base64((from_hex(route))),
                active_start := epoch(r.active_start),
                active_end := epoch(r.active_end)
            )
        ) as routes
    from './data/20251201_20251231/assignments_and_logs.parquet' a
    inner join
        './data/20251201_20251231/routes.parquet' r
        on r.trip_id = a.external_metadata.trip_id
        and r.active_start < a.valid_end
        and a.valid_start < r.active_end
    group by a.pilot_assignment_id
),

map_issues as (
    select
        pilot_assignment_id,
        list(
            struct_pack(
                id := issueid,
                reported_location_hexwkb := st_ashexwkb(st_geomfromgeojson(pointgeo)),
                issue_type := issue_type,
                created_at := epoch(created_at),
                notes := notes
            )
        ) as map_issues
    from './data/20251201_20251231/map_issues.parquet'
    group by pilot_assignment_id
    )

select
    a.pilot_assignment_id,
    a.device_id,
    p.trip_type,
    st_ashexwkb(ST_Point(p.origin_lng, p.origin_lat)) as origin_point_hexwkb,
    st_ashexwkb(ST_Point(p.destination_lng, p.destination_lat)) as destination_point_hexwkb,
    epoch(a.valid_start) as valid_start,
    epoch(a.valid_end) as valid_end,
    a.valid_length_seconds,
    a.n_files,
    a.files as log_files,
    a.external_metadata,
    r.routes,
    i.map_issues
from './data/20251201_20251231/assignments_and_logs.parquet' a
left join './data/20251201_20251231/pilot_trips.parquet' p using (pilot_assignment_id)
left join routes r using (pilot_assignment_id)
left join
    map_issues i using (pilot_assignment_id)
) to './data/20251201_20251231/sqs_messages.parquet' 
;
