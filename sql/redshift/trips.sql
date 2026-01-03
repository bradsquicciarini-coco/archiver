with
    overlapping_assignments as (
        select
            a.trip_id as trip_id_a,
            b.trip_id as trip_id_b,
            a.pilot_assignment_id as pilot_assignment_id_a,
            b.pilot_assignment_id as pilot_assignment_id_b,
            a.trip_started_at,
            a.trip_ended_at,
            b.trip_started_at,
            b.trip_ended_at,
            greatest(a.trip_started_at, b.trip_started_at) as overlap_started_at,
            least(a.trip_ended_at, b.trip_ended_at) as overlap_ended_at,
            datediff('seconds', overlap_started_at, overlap_ended_at) as overlap_seconds
        from dbt_fct.fct_pilot_trips a
        join
            dbt_fct.fct_pilot_trips b
            on a.trip_id <> b.trip_id
            and a.robot_serial = b.robot_serial
            and date_diff('seconds', a.trip_started_at, a.trip_ended_at) > 120
            and a.trip_started_at between '12-01-25' and '12-31-25'
            and a.trip_accepted_at is not null
            and a.trip_started_at < b.trip_ended_at
            and a.trip_ended_at > b.trip_started_at
        where overlap_seconds > 60
        order by overlap_seconds desc
    )

select
    pt.*,
    date_diff('seconds', pt.trip_started_at, pt.trip_ended_at) as trip_length_seconds,
    t.origin_lat,
    t.origin_lng,
    t.destination_lat,
    t.destination_lng
from dbt_fct.fct_pilot_trips pt
join dbt_fct.fct_trips t using (trip_id)
where
    1 = 1
    and pt.trip_started_at between '12-01-25' and '12-31-25'
    -- no incidents
    and (pt.n_pilot_reported_bot_hit is null or pt.n_pilot_reported_bot_hit = 0)
    -- gt 2 mins
    and trip_length_seconds >= 120
    -- filter out overlapping
    and pt.pilot_assignment_id not in (select pilot_assignment_id_a from overlapping_assignments)
order by pt.trip_started_at asc
;
