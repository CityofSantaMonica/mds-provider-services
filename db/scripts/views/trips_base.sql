-- A view of Trips to serve as the basis for other views, with some simple 
-- usability enhancements:
--
--  - convert start_time and end_time to 'America/Los_Angeles' timezone
--  - order results so proviers group together, with the most recently ended trip first

CREATE OR REPLACE VIEW public.v_trips_base AS

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
    timezone('America/Los_Angeles', start_time) AS start_time_local,
    end_time,
    timezone('America/Los_Angeles', end_time) AS end_time_local,
    parking_verification_url,
    standard_cost,
    actual_cost
FROM trips
WHERE trip_duration > 0 AND trip_distance > 0 AND start_time < end_time
ORDER BY provider_name, end_time DESC;