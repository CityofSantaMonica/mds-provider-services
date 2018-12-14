DROP MATERIALIZED VIEW IF EXISTS public.csm_routes CASCADE;

CREATE MATERIALIZED VIEW public.csm_routes AS

SELECT
    provider_id,
    trip_id,
    array_agg(route_point) as route_points,
    st_makeline(array_agg(route_point)) as route_line,
    count(route_point) as total_points,
    sum(CASE WHEN st_contains(csm_city_boundary(), route_point) THEN 1 ELSE 0 END) as in_csm,
    sum(CASE WHEN st_contains(csm_city_boundary(), route_point) THEN 0 ELSE 1 END) as not_in_csm,
    round(sum(CASE WHEN st_contains(csm_city_boundary(), route_point) THEN 1 ELSE 0 END) / count(*)::numeric, 4) as in_csm_pct,
    round(sum(CASE WHEN st_contains(csm_city_boundary(), route_point) THEN 0 ELSE 1 END) / count(*)::numeric, 4) as not_in_csm_pct,
    sum(CASE WHEN st_contains(csm_downtown_district(), route_point) THEN 1 ELSE 0 END) as in_dtsm,
    sum(CASE WHEN st_contains(csm_downtown_district(), route_point) THEN 0 ELSE 1 END) as not_in_dtsm,
    round(sum(CASE WHEN st_contains(csm_downtown_district(), route_point) THEN 1 ELSE 0 END) / count(*)::numeric, 4) as in_dtsm_pct,
    round(sum(CASE WHEN st_contains(csm_downtown_district(), route_point) THEN 0 ELSE 1 END) / count(*)::numeric, 4) as not_in_dtsm_pct
FROM
    route_points
GROUP BY
    provider_id, trip_id
HAVING
    sum(CASE WHEN st_contains(csm_city_boundary(), route_point) THEN 1 ELSE 0 END) > 0

WITH NO DATA;
