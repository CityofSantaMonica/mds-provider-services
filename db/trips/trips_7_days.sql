DROP VIEW IF EXISTS trips_7_days CASCADE;

CREATE VIEW trips_7_days AS

select
    concat(provider_name, '_', vehicle_type) as provider,
    cast(day as date) as day,
    trips
from
    trips_daily
where
    day in (
        select distinct day
        from trips_daily
        order by day desc limit 7
    )
order by
    provider, day;