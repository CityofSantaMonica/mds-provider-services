DROP VIEW IF EXISTS trips_6_months CASCADE;

CREATE VIEW trips_6_months AS

select
    concat(provider_name, '_', vehicle_type) as provider,
    cast(date_trunc('month', day) as date) as month,
    sum(trips) as trips
from
    trips_daily
where
    day in (select distinct day from trips_daily order by day desc limit 180) 
    and day < date_trunc('day', csm_local_timestamp(now() - interval '1 day'))
group by
    provider_name, vehicle_type, date_trunc('month', day)
order by
    provider, month;