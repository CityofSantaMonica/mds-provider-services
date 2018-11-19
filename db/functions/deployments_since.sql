CREATE OR REPLACE FUNCTION public.deployments_since("$1" text, "$2" timestamp without time zone)
    RETURNS TABLE(
        provider_name text,
        device_id uuid,
        vehicle_type vehicle_types,
        propulsion_type propulsion_types[],
        event_time_local timestamp,
        event_type event_types,
        event_type_reason event_type_reasons,
        event_location_geom geometry,
        batter_pct double precision)
    LANGUAGE 'sql'
AS $BODY$

SELECT
    provider_name,
    device_id,
    vehicle_type,
    propulsion_type,
    event_time_local,
    event_type,
    event_type_reason,
    event_location_geom,
    battery_pct
FROM v_status_changes_available
WHERE provider_name = $1
    AND event_time_local >= $2
    AND event_type_reason < 'user_drop_off'::event_type_reasons
ORDER BY event_time_local DESC

$BODY$;