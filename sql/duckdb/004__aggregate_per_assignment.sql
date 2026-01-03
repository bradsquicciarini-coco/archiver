copy (  
with
    source as (select * from read_parquet('./data/20251201_20251231/matched_trips_and_logs.parquet')),
    metadata as (select * from read_parquet('./data/20251201_20251231/metadata.parquet')),
    trip_stats as (
        select
            pilot_assignment_id,
            device_id,
            min(case when log_type = 'video' then start_ts end) as video_start,
            max(case when log_type = 'video' then end_ts end) as video_end,
            min(case when log_type = 'bag' then start_ts end) as bag_start,
            max(case when log_type = 'bag' then end_ts end) as bag_end,
            video_start is not null as has_video,
            bag_start is not null as has_bag,
            greatest(video_start, bag_start) as valid_start,
            least(video_end, bag_end) as valid_end,
            date_diff('seconds', valid_start, valid_end) as valid_length_seconds
        from source
        group by pilot_assignment_id, device_id
        having
            1 = 1  -- data exists on both sides
            and video_start is not null
            and bag_start is not null  -- make sure start is before the end
            and valid_start < valid_end
    ),

    aggregated as (
        select
            t.pilot_assignment_id, t.device_id, t.valid_start, t.valid_end, t.valid_length_seconds, a.n_files, a.files
        from trip_stats t
        join
            lateral(
                select count(*) as n_files, list(key) as files
                from source s
                where
                    s.pilot_assignment_id = t.pilot_assignment_id
                    and s.device_id = t.device_id  -- interval overlap with [valid_start, valid_end]
                    and s.start_ts <= t.valid_end
                    and s.end_ts >= t.valid_start
            ) a
            on true
        order by t.valid_start
    )

select
    a.*,
    struct_pack(
        reference_id := m.pilot_assignment_id,
        trip_id := m.trip_id,
        location := struct_pack(
            city := m.city,
            zone := m.zone,
            country := 'USA',
            local_timezone := m.timezone
        ),
        weather := struct_pack(
            precipitation_mm := m.precipitation,
            temperature_c := m.temperature,
            time_of_day := m.tod,
            cloud_cover := m.cloud_cover,
            weather_icon := m.weather_icon
        ),
        vehicle := struct_pack(
            vehicle_id := a.device_id,
            vehicle_age_days := m.device_age_days,
            vehicle_model := null,
            camera_version := null,
            gps_version := null
        )
    ) as external_metadata
from aggregated as a
join metadata as m using (pilot_assignment_id)
-- where m.city not in ('Helsinki')
    ) to './data/20251201_20251231/assignments_and_logs.parquet' (format 'parquet');
