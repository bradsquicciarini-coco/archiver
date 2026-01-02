with
    src as (
        select * exclude (user_metadata), string_split(trim(both '{}' from user_metadata), ', ') as kv
        from './trip_clips_export.csv'
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
