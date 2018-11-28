CREATE VIEW public.route_metrics AS

SELECT
    provider_name,
    trip_id,
    min(start_time_local) as start_time_local,
    min(end_time_local) as end_time_local,
    round(avg(trip_distance), 0) as trip_distance,
    count(routepoint) as routepoints,
    count(*) filter (where in_csm) as in_csm,
    round((count(*) filter (where in_csm)) / count(*)::numeric, 4) as in_csm_pct,
    count(*) filter (where not in_csm) as not_in_csm,
    round((count(*) filter (where not in_csm)) / count(*)::numeric, 4) as not_in_csm_pct,
    min(timepoint) as min_timepoint,
    max(timepoint) as max_timepoint,
    round(avg(trip_distance), 0) as trip_distance,
    round(avg(reported_duration), 0) as reported_duration,
    round(avg(interval_duration)::numeric, 0) as interval_duration
FROM
    routes
GROUP BY
    provider_name,
    trip_id

;