-- A view of service_start Status Changes.

CREATE MATERIALIZED VIEW public.v_status_changes_available AS

SELECT
    provider_id,
    provider_name,
    device_id,
    vehicle_id,
    vehicle_type,
    propulsion_type,
    event_type,
    event_type_reason,
    event_time,
    timezone('America/Los_Angeles'::text, event_time) AS event_time_local,
    event_location,
    event_location::jsonb as event_location_jsonb,
    battery_pct,
    associated_trips
FROM status_changes
WHERE event_type = 'available'
ORDER BY event_time_local DESC, provider_name

WITH NO DATA;