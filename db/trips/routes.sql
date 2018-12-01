DROP MATERIALIZED VIEW IF EXISTS public.csm_routes CASCADE;

CREATE MATERIALIZED VIEW public.csm_routes AS

SELECT
    provider_id,
    trip_id,
    st_makeline(route_points) as route_line,
    route_points,
    total_points,
    in_csm,
    not_in_csm,
    in_csm_pct,
    not_in_csm_pct,
    in_dtsm,
    not_in_dtsm,
    in_dtsm_pct,
    not_in_dtsm_pct
FROM
    (SELECT
        provider_id,
        trip_id,
        array_agg(route_point) as route_points,
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
        count(route_point) > 2 AND
        sum(CASE WHEN st_contains(csm_city_boundary(), route_point) THEN 1 ELSE 0 END) > 0
    ) r

WITH NO DATA;
