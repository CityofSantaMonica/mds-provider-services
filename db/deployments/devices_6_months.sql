DROP VIEW IF EXISTS devices_6_months CASCADE;

CREATE VIEW devices_6_months AS

select
    concat(provider_name, '_', vehicle_type) as provider,
    cast(date_trunc('month', event_day) as date) as month,
    avg(distinct_devices) as devices
from
    deployments_daily
where
    date_trunc('month', event_day) in (
        select distinct date_trunc('month', event_day) as month
        from deployments_daily
        order by month desc limit 6
    )
group by
    provider_name, vehicle_type, date_trunc('month', event_day)
order by
    provider, month;