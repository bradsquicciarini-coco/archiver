select
    pt.pilot_id, pt.pilot_assignment_id, issueid, issuetype as issue_type, i.submittedat as created_at, pointgeo, notes
from maps_rds_public.issue i
join dbt_fct.fct_pilot_trips pt on pt.trip_id = i.tripid and i.pilotid = pt.pilot_id
where i.submittedat between '2025-01-01' and '2025-11-30'
order by created_at asc
;
