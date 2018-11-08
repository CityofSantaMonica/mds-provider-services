-- A view of Trips with some usability enhancements:
--
--  + convert start_time and end_time to 'America/Los_Angeles' timezone
--  + order results so proviers group together, with the most recently ended trip first

CREATE MATERIALIZED VIEW public.v_trips AS

SELECT
    provider_id,
    provider_name,
    device_id,
    vehicle_id,
    vehicle_type,
    propulsion_type,
    trip_id,
    start_time,
    timezone('America/Los_Angeles'::text, start_time) AS start_time_local,
    end_time,
    timezone('America/Los_Angeles'::text, end_time) AS end_time_local,
    trip_duration,
    trip_duration::numeric / 60.0 AS trip_duration_mins,
    trip_duration::numeric / 3600.0 AS trip_duration_hours,
    trip_distance,
    trip_distance::numeric * 0.0006213712 AS trip_distance_miles,
    trip_distance::numeric / trip_duration::numeric AS avg_speed_ms,
    (trip_distance::numeric / trip_duration::numeric) * 2.236936 AS avg_speed_mph,
    route,
    route::jsonb AS route_jsonb,
    accuracy,
    standard_cost,
    round(standard_cost::numeric / 100.0, 2) AS standard_cost_dollars,
    actual_cost,
    round(actual_cost::numeric / 100.0, 2) AS actual_cost_dollars,
    actual_cost - standard_cost AS actual_cost_difference,
    round((actual_cost - standard_cost)::numeric / 100.0, 2) AS actual_cost_difference_dollars,
    parking_verification_url
FROM trips
WHERE trip_duration > 0 AND trip_distance > 0 AND start_time < end_time
ORDER BY end_time_local DESC, provider_name

WITH NO DATA;