DROP MATERIALIZED VIEW IF EXISTS csm_trips CASCADE;

CREATE MATERIALIZED VIEW csm_trips AS

SELECT
    trips.provider_id,
    trips.provider_name,
    trips.device_id,
    trips.vehicle_type,
    trips.trip_id,
    trips.start_time,
    trips.end_time,
    trips.trip_duration,
    trips.trip_distance,
    trips.standard_cost,
    trips.actual_cost,
    routes.first_csm_timepoint as first_csm_timepoint,
    routes.last_csm_timepoint as last_csm_timepoint,
    routes.first_csm_geopoint,
    routes.last_csm_geopoint,
    routes.route_line
FROM
    trips INNER JOIN routes
        ON trips.provider_id = routes.provider_id
        AND trips.trip_id = routes.trip_id
WHERE
    routes.in_csm_points > 0

WITH NO DATA;
