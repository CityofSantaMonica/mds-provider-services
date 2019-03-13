DROP VIEW IF EXISTS trips_daily CASCADE;

CREATE VIEW trips_daily AS

SELECT
    provider_name,
    vehicle_type,
    date_trunc('day', end_time_local) as day,
    count(*) as trips,
    count(distinct device_id) as devices,
    avg(end_time - start_time) as avg_duration,
    avg(last_csm_timepoint - first_csm_timepoint) as avg_csm_duration
FROM
    csm_trips
GROUP BY
    provider_name,
    vehicle_type,
    date_trunc('day', end_time_local)
ORDER BY
    date_trunc('day', end_time_local) desc,
    provider_name,
    vehicle_type
;