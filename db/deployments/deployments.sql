DROP VIEW IF EXISTS deployments CASCADE;

CREATE VIEW deployments AS

SELECT
    *,
    csm_local_timestamp(event_time) AS event_time_local,
    st_contains(csm_downtown_district(), event_location) as downtown
FROM
    device_event_timeline
WHERE
    event_type = 'available'::event_types
    AND event_type_reason <> 'user_drop_off'::event_type_reasons
;