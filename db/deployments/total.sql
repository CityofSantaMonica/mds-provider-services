CREATE VIEW public.csm_deployments_total AS

SELECT
    provider_name,
    vehicle_type,
    count(DISTINCT device_id) AS unique_devices,
    count(*) AS total_deployments,
    round(count(*) / count(DISTINCT device_id)::numeric, 2) AS deploys_per_device,
    count(DISTINCT date_trunc('day', event_time_local)) AS total_days,
    round(count(DISTINCT device_id) / count(DISTINCT date_trunc('day', event_time_local))::numeric, 2) as devices_per_day,
    round(count(*) / count(DISTINCT date_trunc('day', event_time_local))::numeric, 2) as deploys_per_day,
    count(*) filter (where downtown_deployment = true) as downtown_deployments,
    count(*) filter (where downtown_deployment = false) as non_downtown_deployments,
    round((count(*) filter (where downtown_deployment = true))::numeric / (count(*) filter (where downtown_deployment = false)), 2) as downtown_vs_non_downtown,
    round((count(*) filter (where downtown_deployment = true))::numeric / (count(*)), 2) as downtown_vs_all
FROM
    csm_deployments
GROUP BY
    provider_name, vehicle_type
ORDER BY
    provider_name

;