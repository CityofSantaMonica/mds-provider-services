-- Windows of time a given provider's device was in the public right-of-way

DROP MATERIALIZED VIEW IF EXISTS public.csm_availability CASCADE;

CREATE MATERIALIZED VIEW public.csm_availability AS

SELECT
    provider_id,
    provider_name,
    device_id,
    vehicle_id,
    vehicle_type,
    propulsion_type,
    start_event_type,
    start_reason,
    end_event_type,
    end_reason,
    csm_local_timestamp(start_time) AS start_time_local,
    csm_local_timestamp(end_time) AS end_time_local,
    (end_time - start_time) as delta_time,
    EXTRACT(EPOCH FROM (end_time - start_time)) as delta_seconds,
    round(EXTRACT(EPOCH FROM (end_time - start_time))::numeric / 3600, 2) as delta_hours,
    round(EXTRACT(EPOCH FROM (end_time - start_time))::numeric / 86400, 1) as delta_days,
    csm_parse_feature_geom(event_location) AS event_location,
    st_contains(csm_downtown_district(), csm_parse_feature_geom(event_location)) AS in_csm_downtown,
    start_time,
    end_time
FROM
    public.availability
WHERE
    st_contains(csm_city_boundary(), csm_parse_feature_geom(event_location))
ORDER BY
    (provider_name, vehicle_type, start_time, end_time)

WITH NO DATA;

CREATE UNIQUE INDEX csm_provider_device_window
    ON public.csm_availability (provider_id, device_id, start_time, end_time);
