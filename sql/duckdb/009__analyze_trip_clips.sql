with
    src as (
        select * exclude (user_metadata), string_split(trim(both '{}' from user_metadata), ', ') as kv
        from './data/trip_clips_v3_deduped.parquet'
    ),
    fmt as (
        select
            * exclude (kv),
            map(
                list_transform(kv, x -> split_part(x, '=', 1)), list_transform(kv, x -> split_part(x, '=', 2))
            ) as metadata
        from src
    )

select
    size / (1024 * 1024 * 1024) as size_gb,
    (metadata['clip_duration_seconds'][1])::int as clip_duration_seconds,
    size_gb / (clip_duration_seconds / 3600) as gb_per_hour
from fmt
where not map_contains(metadata, 'error_code')
limit 10
;


copy (
with 

tmp as (
    select
        *,
        _col5::json as user_metadata,
        row_number() over (partition by key order by record_timestamp desc) as rn
    from './data/trip_clips_v3.csv' qualify rn = 1
)
  select 
  
  * exclude (rn), 
  user_metadata->>'__version'                         AS version,
  (user_metadata->>'clip_avg_speed')::DOUBLE          AS clip_avg_speed,
  (user_metadata->>'clip_duration_seconds')::INTEGER  AS clip_duration_seconds,
  (user_metadata->>'clip_start_utc')::TIMESTAMPTZ     AS clip_start_utc,
  (user_metadata->>'clip_end_utc')::TIMESTAMPTZ       AS clip_end_utc,
  user_metadata->>'location__city'                    AS location_city,
  user_metadata->>'location__country'                 AS location_country,
  user_metadata->>'location__local_timezone'          AS location_local_timezone,
  user_metadata->>'location__zone'                    AS location_zone,
  user_metadata->>'reference_id'                      AS reference_id,
  user_metadata->>'trip_id'                           AS trip_id,
  (user_metadata->>'vehicle__camera_version') AS vehicle_camera_version,
  (user_metadata->>'vehicle__gps_version')    AS vehicle_gps_version,
  (user_metadata->>'vehicle__vehicle_age_days')::INTEGER AS vehicle_age_days,
  user_metadata->>'vehicle__vehicle_id'                AS vehicle_id,
  (user_metadata->>'vehicle__vehicle_model')  AS vehicle_model,
  (user_metadata->>'weather__cloud_cover')::DOUBLE     AS weather_cloud_cover,
  (user_metadata->>'weather__precipitation_mm')::DOUBLE AS weather_precipitation_mm,
  (user_metadata->>'weather__temperature_c')::DOUBLE   AS weather_temperature_c,
  user_metadata->>'weather__time_of_day'               AS weather_time_of_day,
  user_metadata->>'weather__weather_icon'              AS weather_weather_icon
  from tmp
) to './data/trip_clips_v3_deduped.parquet';


select t.*
from './data/trip_clips_v3_deduped.parquet' t
left join
