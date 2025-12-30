install spatial
;
load spatial
;

copy (
select
    pilot_assignment_id,
    list(struct_pack(
        reported_location_hexwkb := ST_AsHEXWKB(ST_GeomFromGeojson(pointgeo)),
        issue_type := issue_type,
        created_at := created_at::timestamp,
        id := issueid,
        notes := notes
    )) as map_issues
from './data/20250101_20251130/map_issues.parquet'
group by pilot_assignment_id
) to './data/20250101_20251130/aggregate_map_issues.parquet' (format 'parquet');
