DROP VIEW IF EXISTS devices_6_months CASCADE;

CREATE VIEW devices_6_months AS

select
    concat(provider_name, '_', vehicle_type) as provider,
    cast(date_trunc('month', event_day) as date) as month,
    avg(distinct_devices) as devices
from
    deployments_daily
where
    event_day in (select distinct event_day from deployments_daily order by event_day desc limit 180)
    and event_day < date_trunc('day', csm_local_timestamp(now() - interval '1 day'))
group by
    provider_name, vehicle_type, date_trunc('month', event_day)
order by
    provider, month;