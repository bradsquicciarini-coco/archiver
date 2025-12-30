select *
from dbt_fct.fct_routes_geo
where requested_at between '2025-01-01' and '2025-11-30'
order by requested_at desc
