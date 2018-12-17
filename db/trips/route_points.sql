DROP MATERIALIZED VIEW IF EXISTS public.route_points CASCADE;

CREATE MATERIALIZED VIEW public.route_points AS

SELECT
    trips.provider_id,
    trips.trip_id,
    coords.ts as time_point,
    coords.f as route_point,
    st_contains(csm_city_boundary(), csm_parse_feature_geom(coords.f)) as in_csm
FROM trips CROSS JOIN LATERAL (
    SELECT
        f,
        (f -> 'properties' ->> 'timestamp')::numeric as ts
    FROM jsonb_array_elements(trips.route -> 'features') f
) coords
ORDER BY
    provider_id,
    trip_id,
    time_point

WITH NO DATA;