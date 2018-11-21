CREATE MATERIALIZED VIEW public.v_trip_routes AS

SELECT
    provider_name,
    device_id,
    vehicle_type,
    trip_id,
    start_time AS trip_start_time,
    timezone('America/Los_Angeles', start_time) AS trip_start_time_local,
    end_time AS trip_end_time,
    timezone('America/Los_Angeles', end_time) AS trip_end_time_local,
    timezone('America/Los_Angeles', to_timestamp(((jsonb_array_elements(route -> 'features') -> 'properties') ->> 'timestamp')::numeric) AT TIME ZONE 'UTC') AS timepoint_local,
    st_setsrid(st_geomfromgeojson(jsonb_array_elements(route -> 'features') ->> 'geometry'), 4326) AS geometry,
    accuracy AS route_accuracy,
    trip_distance,
    trip_duration
FROM
    trips
ORDER BY
    trip_end_time_local DESC, trip_id, timepoint_local

WITH NO DATA;