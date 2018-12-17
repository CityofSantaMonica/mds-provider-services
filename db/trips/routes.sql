DROP MATERIALIZED VIEW IF EXISTS public.csm_routes CASCADE;

CREATE MATERIALIZED VIEW public.csm_routes AS

SELECT
    provider_id,
    trip_id,
    min(time_point) filter (where in_csm) as first_csm_time,
    max(time_point) filter (where in_csm) as last_csm_time,
    ((array_agg(route_point) filter (where in_csm))::jsonb[])[1] as first_csm_point
FROM
    route_points
GROUP BY
    provider_id, trip_id
HAVING
    count(*) filter (where in_csm) > 0

WITH NO DATA;
