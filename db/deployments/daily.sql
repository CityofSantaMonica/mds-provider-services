CREATE VIEW public.csm_deployments_daily AS

SELECT
    provider_name,
    date_trunc('day', event_time_local) AS event_day,
    vehicle_type,
    count(DISTINCT device_id) AS unique_devices,
    count(*) AS total_deployments,
    round(count(*) / count(DISTINCT device_id)::numeric, 2) AS deploys_per_device,
    count(DISTINCT date_trunc('hour', event_time_local)) AS hours_active,
    round(count(*) / count(DISTINCT date_trunc('hour', event_time_local))::numeric, 2) AS deploys_per_hour,
    round(count(DISTINCT device_id) / count(DISTINCT date_trunc('hour', event_time_local))::numeric, 2) AS devices_per_hour,
    count(*) filter (where downtown_deployment = true) AS downtown_deployments,
    count(*) filter (where downtown_deployment = false) AS non_downtown_deployments,
    round((count(*) filter (where downtown_deployment = true))::numeric / (count(*) filter (where downtown_deployment = false)), 2) AS downtown_vs_non_downtown,
    round((count(*) filter (where downtown_deployment = true))::numeric / count(*), 2) AS downtown_vs_all
FROM
    csm_deployments
GROUP BY
    provider_name, event_day, vehicle_type
ORDER BY
    event_day DESC, provider_name