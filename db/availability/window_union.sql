-- Windows of time a given provider's device was in the public right-of-way

CREATE VIEW public.window_union AS

SELECT
    provider_id,
    provider_name,
    vehicle_type,
    propulsion_type,
    device_id,
    vehicle_id,
    event_location,
    start_event_type,
    end_event_type,
    start_reason,
    end_reason,
    start_time,
    end_time
FROM
    available_windows

UNION

SELECT
    provider_id,
    provider_name,
    vehicle_type,
    propulsion_type,
    device_id,
    vehicle_id,
    event_location,
    start_event_type,
    end_event_type,
    start_reason,
    end_reason,
    start_time,
    end_time
FROM
    trip_windows

;