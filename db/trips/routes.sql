CREATE MATERIALIZED VIEW public.routes AS

SELECT
    provider_id,
    vehicle_type,
    device_id,
    trip_id,
    trip_distance,
    trip_duration AS reported_duration,
    EXTRACT(EPOCH FROM (end_time - start_time)) AS interval_duration,
    csm_local_timestamp(start_time) AS start_time_local,
    csm_local_timestamp(end_time) AS end_time_local,
    ((jsonb_array_elements(trips.route -> 'features') -> 'properties') ->> 'timestamp')::numeric AS timepoint,
    timezone('UTC', to_timestamp((((jsonb_array_elements(trips.route -> 'features') -> 'properties') ->> 'timestamp')::numeric))) AS timepoint_timestamp,
    csm_local_timestamp(to_timestamp((jsonb_array_elements(route -> 'features') -> 'properties' ->> 'timestamp')::numeric)) AS timepoint_timestamp_local,
    csm_parse_feature_geom(jsonb_array_elements(route -> 'features')) AS routepoint,
    st_contains(csm_city_boundary(), csm_parse_feature_geom(jsonb_array_elements(route -> 'features'))) as in_csm,
    accuracy AS route_accuracy,
    start_time,
    end_time
FROM
    trips
ORDER BY
    end_time DESC, provider_id, trip_id, timepoint

WITH NO DATA;