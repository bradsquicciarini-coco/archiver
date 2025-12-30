copy (
  select
    'coco-gg-bags-prod' as bucket,
    key as object_key
  from './data/matched_trips_and_logs.parquet'
) to './data/manifest_20250101_20251130.csv' (format 'csv', header false);
