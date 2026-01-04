with
    tmp as (
        select
            (t.user_metadata ->> 'clip_duration_seconds')::int as clip_duration_seconds,
            t.user_metadata ->> 'reference_id' as pilot_assignment_id,
            (t.user_metadata ->> 'clip_avg_speed')::float as clip_avg_speed,
            p.* exclude (pilot_assignment_id)
        from './trip_clips_deduped.parquet' t
        join './manifests.parquet' p on p.pilot_assignment_id = (t.user_metadata ->> 'reference_id')
    )
select
    sum(clip_duration_seconds) / 3600 as total_hours,
    sum(case when clip_avg_speed != 0 then clip_duration_seconds else 0 end) / 3600 as total_hours,
    count(*) as total_clips,
    count(distinct pilot_assignment_id) as total_assignments
from tmp
;

-- pull out keys for tar files between [1, 6]
copy (select
    t.user_metadata ->> 'reference_id' as pilot_assignment_id,
    from_json(t.user_metadata, '{
  "__version": "INTEGER",
  "clip_avg_speed": "DOUBLE",
  "clip_duration_seconds": "INTEGER",
  "clip_end_utc": "TIMESTAMP",
  "clip_start_utc": "TIMESTAMP",
  "location__city": "VARCHAR",
  "location__country": "VARCHAR",
  "location__local_timezone": "VARCHAR",
  "location__zone": "VARCHAR",
  "reference_id": "UUID",
  "trip_id": "UUID",
  "vehicle__camera_version": "VARCHAR",
  "vehicle__gps_version": "INTEGER",
  "vehicle__vehicle_age_days": "INTEGER",
  "vehicle__vehicle_id": "VARCHAR",
  "vehicle__vehicle_model": "VARCHAR",
  "weather__cloud_cover": "DOUBLE",
  "weather__precipitation_mm": "DOUBLE",
  "weather__temperature_c": "DOUBLE",
  "weather__time_of_day": "VARCHAR",
  "weather__weather_icon": "VARCHAR"
}') as user_metadata_parsed
from './trip_clips_deduped.parquet' t
join './manifests.parquet' p on p.pilot_assignment_id = (t.user_metadata ->> 'reference_id')
where date_part('month', (regexp_extract(p.tarname, '^(\d{4}-\d{2})', 1) || '-01')::date) between 1 and 6
) to './tar-metadata.parquet';


-- build messages
copy (
with tmp as (
select
distinct tarname
from './manifests.parquet' p
where date_part('month', (regexp_extract(p.tarname, '^(\d{4}-\d{2})', 1) || '-01')::date) between 1 and 6
)

select 'coco-trip-clips-976053906881-us-west-2' as bucket, 'tar/' || tarname  || '.tar' as key 
from tmp
) to './tar-messages.parquet';
