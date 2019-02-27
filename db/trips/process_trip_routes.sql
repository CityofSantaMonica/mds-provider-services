DROP FUNCTION IF EXISTS csm_process_trip_routes();

CREATE OR REPLACE FUNCTION csm_process_trip_routes(OUT route_points_start_id bigint, OUT route_points_end_id bigint,
                                                   OUT routes_start_id bigint, OUT routes_end_id bigint)
RETURNS record
LANGUAGE plpgsql
AS $function$
BEGIN
    SELECT start_id, end_id INTO route_points_start_id, route_points_end_id
    FROM csm_classify_route_points();

    SELECT start_id, end_id INTO routes_start_id, routes_end_id
    FROM csm_aggregate_routes();
END;
$function$;