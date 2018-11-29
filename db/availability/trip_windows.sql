-- Windows of time a given provider's device was active in a trip

CREATE VIEW public.trip_windows AS

SELECT
    pickup.provider_id,
    pickup.provider_name,
    pickup.vehicle_type,
    pickup.propulsion_type,
    pickup.device_id,
    pickup.vehicle_id,
    pickup.event_location AS event_location,
    pickup.event_type AS start_event_type,
    next_event.event_type AS end_event_type,
    pickup.event_type_reason AS start_reason,
    next_event.event_type_reason AS end_reason,
    pickup.event_time AS start_time,
    next_event.event_time AS end_time
FROM
    device_event_timeline pickup LEFT JOIN device_event_timeline next_event
    ON pickup.event_type_reason = 'user_pick_up'::event_type_reasons
    AND pickup.provider_id = next_event.provider_id
    AND pickup.device_id = next_event.device_id
    AND (pickup.row_num + 1) = next_event.row_num
WHERE
    pickup.event_type_reason = 'user_pick_up'::event_type_reasons

;