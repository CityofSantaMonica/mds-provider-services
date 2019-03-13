DROP VIEW IF EXISTS deployments_daily CASCADE;

CREATE VIEW deployments_daily AS

SELECT
    provider_name,
    vehicle_type,
    date_trunc('day', event_time_local) AS event_day,
    count(distinct device_id) AS distinct_devices,
    count(*) AS deployments,
    round(count(*) / count(distinct device_id)::numeric, 2) AS deploys_per_device,
    count(distinct date_trunc('hour', event_time_local)) AS hours_active,
    round(count(*) / count(distinct date_trunc('hour', event_time_local))::numeric, 2) AS deploys_per_hour,
    round(count(distinct device_id) / count(distinct date_trunc('hour', event_time_local))::numeric, 2) AS devices_per_hour,
    count(*) filter (where downtown) AS downtown_deployments,
    count(*) filter (where not downtown) AS non_downtown_deployments,
    round((count(*) filter (where downtown))::numeric / count(*), 2) AS downtown_pct
    round((count(*) filter (where not downtown))::numeric / count(*), 2) AS non_downtown_pct
FROM
    deployments
GROUP BY
    provider_name, date_trunc('day', event_time_local), vehicle_type
ORDER BY
    event_day DESC, provider_name, vehicle_type
;