DROP MATERIALIZED VIEW IF EXISTS public.csm_deployments CASCADE;

CREATE MATERIALIZED VIEW public.csm_deployments AS

SELECT
    provider_id,
    provider_name,
    device_id,
    vehicle_id,
    vehicle_type,
    propulsion_type,
    event_type,
    event_type_reason,
    event_time,
    csm_local_timestamp(event_time) AS event_time_local,
    event_location,
    csm_parse_feature_geom(event_location) AS event_location_geom,
    st_contains(csm_downtown_district(), csm_parse_feature_geom(event_location)) as downtown_deployment,
    battery_pct,
    associated_trips
FROM
    deployments
WHERE
    st_contains(csm_city_boundary(), csm_parse_feature_geom(event_location))

WITH NO DATA;

CREATE UNIQUE INDEX provider_device_event
    ON public.csm_deployments (provider_id, device_id, event_time);
