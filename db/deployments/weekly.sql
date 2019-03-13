DROP VIEW IF EXISTS deployments_weekly CASCADE;

CREATE VIEW deployments_weekly AS

SELECT
    provider_name,
    vehicle_type,
    date_trunc('week', event_time_local) AS event_week,
    count(distinct device_id) AS distinct_devices,
    count(*) AS deployments,
    round(count(*) / count(distinct device_id)::numeric, 2) AS deploys_per_device,
    count(distinct date_trunc('day', event_time_local)) AS total_days,
    round(count(distinct device_id) / count(distinct date_trunc('day', event_time_local))::numeric, 2) as devices_per_day,
    round(count(*) / count(distinct date_trunc('day', event_time_local))::numeric, 2) as deploys_per_day,
    count(*) filter (where downtown) as downtown_deployment,
    round((count(*) filter (where downtown))::numeric / (count(*)), 2) as downtown_deployment_pct,
    count(*) filter (where not downtown) as non_downtown_deployment,
    round((count(*) filter (where not downtown))::numeric / (count(*)), 2) as non_downtown_deployment_pct
FROM
    deployments
GROUP BY
    provider_name, vehicle_type, date_trunc('week', event_time_local)
ORDER BY
    event_week DESC, provider_name, vehicle_type

;