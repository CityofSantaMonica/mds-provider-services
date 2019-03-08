-- Windows of time a given provider's device was active in a trip

DROP MATERIALIZED VIEW IF EXISTS active_windows CASCADE;

CREATE MATERIALIZED VIEW active_windows AS

SELECT
    provider_id,
    provider_name,
    vehicle_type,
    device_id,
    first_csm_timepoint as start_time,
    last_csm_timepoint as end_time,
    first_csm_geopoint AS event_location,
    'reserved'::event_types AS start_event_type,
    'available'::event_types AS end_event_type,
    'user_pick_up'::event_type_reasons AS start_reason,
    'user_drop_off'::event_type_reasons AS end_reason
FROM
    csm_trips

WITH NO DATA
;
