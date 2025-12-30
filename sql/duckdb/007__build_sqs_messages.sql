install spatial
;
load spatial
;

copy (
select
    a.pilot_assignment_id,
    a.device_id,
    a.valid_start,
    a.valid_end,
    a.valid_length_seconds,
    a.n_files,
    a.files as log_files,
    a.external_metadata,
    r.routes,
    i.map_issues
from './data/20250101_20251130/assignments_and_logs.parquet' a
left join './data/20250101_20251130/matched_assignments_and_routes.parquet' r using (pilot_assignment_id)
left join './data/20250101_20251130/aggregate_map_issues.parquet' i using (pilot_assignment_id)
where a.valid_start >= '2025-11-30'::timestamp
limit 10
) to './data/20250101_20251130/sqs_messages.jsonl'
;
