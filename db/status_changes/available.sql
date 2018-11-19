-- A view of event_type: 'available' Status Changes.

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
    event_location -> 'properties' ->> 'timestamp'::timestamptz at time zone 'UTC' as event_timestamp,
    timezone('America/Los_Angeles'::text, event_time) AS event_time_local,
    event_location,
    st_setsrid(st_geomfromgeojson((event_location -> 'geometry'::text)::text), 4326) AS event_location_geom,
    battery_pct,
    associated_trips
  FROM status_changes
  WHERE event_type = 'available'::event_types
  ORDER BY (timezone('America/Los_Angeles'::text, event_time)) DESC, provider_name

WITH NO DATA;