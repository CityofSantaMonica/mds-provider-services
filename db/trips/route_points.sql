DROP VIEW IF EXISTS public.route_points CASCADE;

CREATE VIEW public.route_points AS

SELECT
    trips.provider_id,
    trips.trip_id,
    coords.ts as time_point,
    csm_parse_feature_geom(coords.f) as route_point,
    coords.f as route_point_feature
FROM trips CROSS JOIN LATERAL (
    SELECT
        f,
        (f -> 'properties' ->> 'timestamp')::numeric as ts
    FROM jsonb_array_elements(trips.route -> 'features') f
    ORDER BY ts
) coords

;