DROP MATERIALIZED VIEW IF EXISTS public.route_points CASCADE;

CREATE MATERIALIZED VIEW public.route_points AS

SELECT
    provider_id,
    trip_id,
    ((jsonb_array_elements(trips.route -> 'features') -> 'properties') ->> 'timestamp')::numeric AS time_point,
    timezone('UTC', to_timestamp((((jsonb_array_elements(trips.route -> 'features') -> 'properties') ->> 'timestamp')::numeric))) AS time_point_timestamp,
    csm_parse_feature_geom(jsonb_array_elements(route -> 'features')) AS route_point,
    jsonb_array_elements(route -> 'features') AS route_point_feature
FROM
    trips

WITH NO DATA;

CREATE INDEX provider_trip
    ON public.route_points (provider_id, trip_id);