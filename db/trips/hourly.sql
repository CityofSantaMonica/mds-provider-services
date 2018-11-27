-- An hour-by-hour view of Trips per provider

CREATE VIEW public.trips_hourly AS

SELECT
    provider_name,
    date_trunc('hour', csm_local_timestamp(event_time)) AS end_hour_local,
    vehicle_type,
    count(trip_id) AS total_trips,
    count(DISTINCT device_id) as unique_devices,
    round(count(trip_id)::numeric / count(DISTINCT device_id), 2) as trips_per_device,
    round(avg(trip_duration), 1) AS avg_trip_duration,
    round(avg(trip_duration::numeric / 60.0), 4) AS avg_trip_duration_mins,
    round(avg(trip_duration::numeric / 3600.0), 4) AS avg_trip_duration_hours,
    sum(trip_duration) AS total_trip_duration,
    sum(trip_duration::numeric / 60.0) AS total_trip_duration_mins,
    sum(trip_duration::numeric / 3600.0) AS total_trip_duration_hours,
    round(avg(trip_distance), 1) AS avg_trip_distance,
    round(avg(trip_distance * 0.0006213712), 4) AS avg_trip_distance_miles,
    sum(trip_distance) AS total_trip_distance,
    sum(trip_distance * 0.0006213712) AS total_trip_distance_miles,
    round(avg(actual_cost), 0) AS avg_trip_cost,
    round(avg(actual_cost::numeric / 100.0), 2) AS avg_trip_cost_dollars,
    sum(actual_cost) AS total_trip_revenue,
    round(sum(actual_cost::numeric / 100.0), 2) AS total_trip_revenue_dollars
FROM
    public.trips
GROUP BY
    provider_name, end_hour_local, vehicle_type
ORDER BY
    end_hour_local DESC, provider_name

;