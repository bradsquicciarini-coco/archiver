select
    pilot_assignment_id,
    device_id,
    chunk_start,
    chunk_end,
    chunk_id,
    round(avg_speed_meters_per_second, 2) as avg_speed_mps
from dbt_reports.driving_data__trip_chunks_stats
where
    trip_type in ('DEPLOYMENT_TRIP', 'RETURN_TRIP')
    or (trip_type in ('DELIVERY_TRIP') and dist_from_stay_away_dest_m > 50 and dist_from_stay_away_origin_m > 50)
    or (trip_type in ('JITP_TRIP') and dist_from_stay_away_dest_m > 50)
