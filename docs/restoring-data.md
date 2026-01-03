## TODO

- should have included object size during inventory

## Inventory

The goal of this step is to inventory what is in s3 and create a structured representation so we can determine which logs
are associated with which trips.

```bash
# 1) inventory the bucket
uv run scripts/make-inventory.py coco-gg-bags-prod --workers 16 --profile prod --out ./data/20251216-coco-gg-bags-prod-inventory

# 2) parse out timestamps, log type, device etc from filename. This saves parsed object keys to logs.parquet
 duckdb :memory: < sql/duckdb/001__parse_log_filenames.sql

# 3) Check how much video data is available
duckdb :memory: "
select
  sum(case when log_type = 'bag' then log_duration_min else 0 end) / 60 as bag_duration,
  sum(case when log_type = 'video' then log_duration_min else 0 end) / 60 as video_duration,
  video_duration / bag_duration as video_bag_ratio
from './data/20251201_20251231/logs.parquet';
"
```

## Trip Matching

```bash
uv run  scripts/query-redshift.py sql/redshift/trips.sql --profile prod -o data/20250101_20251130/pilot_trips.parquet
duckdb :memory: < sql/duckdb/002__match_trips_and_logs.sql
duckdb :memory: < sql/duckdb/004__aggregate_per_assignment.sql

# check video data
duckdb :memory: "
select
  city,
  sum(trip_length_seconds)
from './data/20250101_20251130/pilot_trips.parquet'
where trip_id in (
  select distinct trip_id from './data/20250101_20251130/matched_trips_and_logs.parquet'
);
"
```

NOTE: the `trip -> log file` mapping is NOT 1-to-1. This happens because a trip can start and end in the same log file.
Right now this causes some `RestoreAlreadyInProgress` errors, when performing a batch restore. These should be deduped
from the manifest to avoid this.

## Generate manifest

```bash
# upload manifest to s3
# BUG: this has some duplicates
aws s3 --profile prod cp ./data/manifest_20250101_20251130.csv s3://athena-express-asia6gqkyv-2022/data/manifest_20250101_20251130.csv

# start a batch job
# This was done from the console.
```

## Prepare for SQS Queue

```bash
duckdb :memory: < sql/duckdb/004__aggregate_per_assignment.sql
```

```sql
-- for parsing route
 ST_GeomFromWKB(from_hex(route))
```

```bash
duckdb :memory: < sql/duckdb/005__match_routes_and_trips.sql
```

These seem to be well under 1 MB

```sql
copy (
  select
      pilot_assignment_id,
      list(
        struct_pack(
          geom := route_geom,
          active_start := active_start,
          active_end := active_end
        )
      ) as routes
  from './data/20250101_20251130/matched_assignments_and_routes.parquet'
  where pilot_assignment_id = 'fd978b12-4b1b-46c8-be0b-6e924d73448a'
  group by pilot_assignment_id
) to 'test.jsonl';
```
