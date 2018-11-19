-- A view of non-user-initiated event_type: 'available' Status Changes,
-- aggregating unique devices per hour.

CREATE MATERIALIZED VIEW public.v_status_changes_deployment_daily AS

    SELECT
        provider_name,
        event_type,
        event_type_reason,
        count(DISTINCT device_id) AS unique_devices,
        date_trunc('day'::text, timezone('America/Los_Angeles'::text, event_time)) AS event_day,
        count(DISTINCT (device_id, event_type_reason, event_time)) AS unique_events
    FROM
        status_changes
    WHERE
        event_type = 'available'::event_types
        AND event_type_reason < 'user_drop_off'::event_type_reasons
    GROUP BY 
        provider_name,
        event_day,
        event_type,
        event_type_reason
    ORDER BY event_day DESC, unique_devices DESC;

WITH NO DATA;