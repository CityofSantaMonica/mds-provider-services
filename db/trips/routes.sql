CREATE MATERIALIZED VIEW public.v_trips_routes AS

SELECT
    provider_name,
    device_id,
    trip_id,
    csm_local_timestamp(start_time) AS start_time_local,
    csm_local_timestamp(end_time) AS end_time_local,
    jsonb_array_elements(route -> 'features') -> 'properties') ->> 'timestamp')::numeric AS timepoint,
    to_timestamp(((jsonb_array_elements(route -> 'features') -> 'properties') ->> 'timestamp')::numeric) AT TIME ZONE 'UTC' AS timepoint_timestamp,
    csm_local_timestamp(to_timestamp(((jsonb_array_elements(route -> 'features') -> 'properties') ->> 'timestamp')::numeric)) AS timepoint_timestamp_local,
    csm_parse_feature_geom(jsonb_array_elements(route -> 'features')) AS routepoint,
    accuracy AS route_accuracy,
    trip_distance,
    trip_duration AS reported_duration,
    EXTRACT(EPOCH FROM (end_time - start_time)) AS interval_duration
FROM
    trips
ORDER BY
    trip_end_time_local DESC, provider_id, trip_id, timepoint_local

WITH NO DATA;