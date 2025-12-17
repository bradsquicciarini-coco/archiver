select
  robot_serial as device_name,
  pilot_name,
  trip_started_at as start_ts,
trip_ended_at as end_ts,
  date_diff('seconds', trip_started_at, trip_ended_at) as length_seconds
from
  dbt_fct.fct_pilot_trips
where
  trip_started_at < getdate () - interval '12 hours'
  and length_seconds > 120
order by
  trip_started_at desc
 limit 5000
offset 500
