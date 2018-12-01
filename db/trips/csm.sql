DROP VIEW IF EXISTS public.csm_trips CASCADE;

CREATE VIEW public.csm_trips AS

SELECT
    trips.provider_id,
    trips.provider_name,
    trips.device_id,
    trips.vehicle_id,
    trips.vehicle_type,
    trips.propulsion_type,
    trips.trip_id,
    trips.trip_duration,
    trips.trip_distance,
    trips.route,
    trips.accuracy,
    trips.start_time,
    trips.end_time,
    trips.parking_verification_url,
    trips.standard_cost,
    trips.actual_cost,
    csm_local_timestamp(trips.start_time) as start_time_local,
    csm_local_timestamp(trips.end_time) as end_time_local,
    csm_routes.route_line,
    csm_routes.route_points,
    csm_routes.total_points,
    csm_routes.in_csm,
    csm_routes.not_in_csm,
    csm_routes.in_csm_pct,
    csm_routes.not_in_csm_pct,
    csm_routes.in_dtsm,
    csm_routes.not_in_dtsm,
    csm_routes.in_dtsm_pct,
    csm_routes.not_in_dtsm_pct
FROM
    trips INNER JOIN csm_routes
        ON trips.provider_id = csm_routes.provider_id
        AND trips.trip_id = csm_routes.trip_id
ORDER BY
    provider_name,
    vehicle_type,
    end_time_local DESC

;
