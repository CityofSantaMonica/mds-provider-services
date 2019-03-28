DROP VIEW IF EXISTS trips_6_months CASCADE;

CREATE VIEW trips_6_months AS

select
    concat(provider_name, '_', vehicle_type) as provider,
    cast(date_trunc('month', day) as date) as month,
    sum(trips) as trips
from
    trips_daily
where
    date_trunc('month', day) in (
        select distinct date_trunc('month', day) as month
        from trips_daily
        order by month desc limit 6
    )
group by
    provider_name, vehicle_type, date_trunc('month', day)
order by
    provider, month;