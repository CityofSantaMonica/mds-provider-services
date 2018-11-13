-- A view of service_start Status Changes.

CREATE MATERIALIZED VIEW public.v_status_changes_daily AS

SELECT
    provider_id,
    provider_name,
    date_trunc('day', timezone('America/Los_Angeles'::text, event_time)) AS event_day_local,
    event_type,
    event_type_reason,
    count(*) as events_count
FROM status_changes
GROUP BY provider_id, provider_name, event_day_local, event_type, event_type_reason
ORDER BY event_day_local DESC, provider_name, event_type

WITH NO DATA;