install spatial
;
load spatial
;
pragma memory_limit = '14GB'
;
pragma temp_directory = '/tmp/duckdb_spill'
;
copy (
    select 
        a.pilot_assignment_id,
        list(struct_pack(
            geom_b64 := base64((from_hex(route))),
            active_start := r.active_start::timestamp, 
            active_end := r.active_end::timestamp, 
            id := r.internal_routing_id)
        ) as routes
    from './data/20250101_20251130/assignments_and_logs.parquet' a
    left join
        './data/20250101_20251130/routes.parquet' r
        on r.trip_id = a.external_metadata.trip_id
        and r.active_start < a.valid_end
        and a.valid_start < r.active_end
    group by a.pilot_assignment_id
) to './data/20250101_20251130/matched_assignments_and_routes.parquet' (format 'parquet');
