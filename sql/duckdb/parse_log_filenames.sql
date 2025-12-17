copy (
  with tmp as (
    select
      nullif(regexp_extract(key, '\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}'), '') as ts_str0,
      nullif(replace(regexp_extract(key, '\d{4}-\d{2}-\d{2}_\d{2}_\d{2}_\d{2}'), '_', '-'), '') as ts_str1,
      coalesce(ts_str1, ts_str0) as ts_str,
      try_strptime(ts_str, '%Y-%m-%d-%H-%M-%S') as start_ts,
      regexp_extract(key, 'C1\d{4}') as device_id,
      key,
      case
        when key like '%.bag' then 'bag'
        when key like '%_sess_%' then 'driveu_session'
        when key like '%_h264.mcap' then 'video'
        else 'unknown'
      end as log_type,
      case
        when log_type == 'bag' then 10
        when log_type == 'video' then 1
      end as log_duration_min,
      date_add(start_ts, log_duration_min * INTERVAL '1 minute') as end_ts
    from './data/20251216-coco-gg-bags-prod-inventory/*.parquet'
  )

  select
    device_id,
    key,
    log_type,
    log_duration_min,
    start_ts,
    end_ts,
  from tmp
  where 
    start_ts is not null 
    and device_id is not null
    and start_ts between '2025-01-01' and '2025-12-15'
) to './data/logs.parquet';
