copy (
  select
    'coco-gg-bags-prod' as bucket,
    key as object_key
  from './data/20251201_20251231/matched_trips_and_logs.parquet'
  where start_ts::date between '2025-12-01' and '2025-12-17'
) to './data/20251201_20251231/glacier_restore_manifest.csv' (format 'csv', header false);
