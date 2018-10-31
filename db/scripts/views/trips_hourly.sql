-- An hour-by-hour view of Trips per provider

CREATE VIEW public.v_trips_hourly AS

SELECT
    provider_id,
    provider_name,
    date_trunc('hour', end_time_local) AS end_hour_local,
    count(trip_id) AS trips_count,
    avg(trip_duration) AS avg_trip_duration,
    round(avg(trip_duration / 60.0), 4) AS avg_trip_duration_min,
    avg(trip_distance) AS avg_trip_distance,
    avg(actual_cost) AS avg_trip_cost,
    round(avg(actual_cost / 100.0), 2) AS avg_trip_cost_dollar,
    sum(actual_cost) AS total_trip_revenue,
    round(sum(actual_cost) / 100.0, 2) AS total_trip_revenue_dollar
FROM public.v_trips_base
GROUP BY provider_id, provider_name, end_hour_local
ORDER BY end_hour_local DESC, provider_name;