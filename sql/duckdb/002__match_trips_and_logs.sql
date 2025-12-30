copy (select t.trip_id, t.pilot_assignment_id, l.*
from './data/logs.parquet' l
join
    'data/pilot_trips_20250101_20251130.parquet' t
    on l.device_id = t.robot_serial
    and l.start_ts < t.trip_ended_at
    and l.end_ts > t.trip_started_at
) to './data/matched_trips_and_logs.parquet' (format 'parquet');
