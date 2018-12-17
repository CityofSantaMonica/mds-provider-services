-- Windows of time a given provider's device was in the public right-of-way

DROP VIEW IF EXISTS public.csm_availability CASCADE;

CREATE VIEW public.csm_availability AS

SELECT
    provider_id,
    provider_name,
    device_id,
    vehicle_type,
    start_event_type,
    start_reason,
    end_event_type,
    end_reason,
    start_time,
    end_time,
    csm_local_timestamp(start_time) AS start_time_local,
    csm_local_timestamp(end_time) AS end_time_local,
    csm_parse_feature_geom(event_location) AS event_location
FROM
    public.availability
ORDER BY
    (provider_name, vehicle_type, start_time, end_time)

;