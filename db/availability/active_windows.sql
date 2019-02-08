-- Windows of time a given provider's device was active in a trip

DROP VIEW IF EXISTS public.active_windows CASCADE;

CREATE VIEW public.active_windows AS

SELECT
    provider_id,
    provider_name,
    vehicle_type,
    device_id,
    to_timestamp(first_csm_time) as start_time,
    to_timestamp(last_csm_time) as end_time,
    first_csm_point AS event_location,
    'reserved'::event_types AS start_event_type,
    'available'::event_types AS end_event_type,
    'user_pick_up'::event_type_reasons AS start_reason,
    'user_drop_off'::event_type_reasons AS end_reason
FROM
    csm_trips
;
