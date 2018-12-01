-- Each event, for each device, for each provider - ordered chronologically

DROP VIEW IF EXISTS public.device_event_timeline CASCADE;

CREATE VIEW public.device_event_timeline AS

SELECT
    row_number() OVER (ORDER BY provider_name, vehicle_type, device_id, event_time) AS row_num,
    *
FROM
    status_changes
ORDER BY
    provider_name,
    row_num

;