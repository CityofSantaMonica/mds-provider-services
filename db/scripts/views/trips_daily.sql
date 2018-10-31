-- An day-by-day view of Trips per provider

CREATE VIEW public.v_trips_daily AS

SELECT
    provider_id,
    provider_name,
    date_trunc('day', end_time_local) AS end_day_local,
    count(trip_id) AS trips_count,
    avg(trip_duration) AS avg_trip_duration,
    round(avg(trip_duration / 60.0), 4) AS avg_trip_duration_min,
    avg(trip_distance) AS avg_trip_distance,
    avg(actual_cost) AS avg_trip_cost,
    round(avg(actual_cost / 100.0), 2) AS avg_trip_cost_dollar,
    sum(actual_cost) AS total_trip_revenue,
    round(sum(actual_cost) / 100.0, 2) AS total_trip_revenue_dollar
FROM public.v_trips_base
GROUP BY provider_id, provider_name, end_day_local
ORDER BY end_day_local DESC, provider_name;