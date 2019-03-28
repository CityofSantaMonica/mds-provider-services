DROP VIEW IF EXISTS lost_daily CASCADE;

CREATE VIEW lost_daily AS

select
    provider_name,
    vehicle_type,
    cast(date_trunc('day', start_time_local) as date) as day,
    count(distinct device_id) as lost_devices
from
    lost_devices
group by
    provider_name, vehicle_type, date_trunc('day', start_time_local)
order by
    provider_name, vehicle_type, day;
