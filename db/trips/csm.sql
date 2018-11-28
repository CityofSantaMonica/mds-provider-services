CREATE MATERIALIZED VIEW public.csm_trips AS

SELECT
    provider_id,
    provider_name,
    device_id,
    vehicle_id,
    vehicle_type,
    propulsion_type,
    trip_id,
    trip_duration,
    trip_distance,
    route,
    accuracy,
    start_time,
    end_time,
    csm_local_timestamp(start_time) as start_time_local,
    csm_local_timestamp(end_time) as end_time_local,
    parking_verification_url,
    standard_cost,
    actual_cost
FROM
    trips
WHERE
    (provider_name, trip_id) IN (SELECT provider_name, trip_id FROM route_metrics WHERE in_csm > 0)
ORDER BY
    provider_name,
    vehicle_type,
    end_time_local DESC

WITH NO DATA;