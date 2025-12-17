## TODO

- should have included object size during inventory

## Inventory

The goal of this step is to inventory what is in s3 and create a structured representation so we can determine which logs
are associated with which trips.

```bash
# 1) inventory the bucket
uv run scripts/make-inventory.py coco-gg-bags-prod --workers 16 --profile prod --out ./data/20251216-coco-gg-bags-prod-inventory

# 2) parse out timestamps, log type, device etc from filename. This saves parsed object keys to logs.parquet
duckdb :memory: < sql/duckdb/parse_log_filenames.sql

# 3) Check how much video data is available
duckdb :memory: "
select
  sum(case when log_type = 'bag' then log_duration_min else 0 end) / 60 as bag_duration,
  sum(case when log_type = 'video' then log_duration_min else 0 end) / 60 as video_duration,
  video_duration / bag_duration as video_bag_ratio
from './data/logs.parquet';
"
```

## Trip Matching
