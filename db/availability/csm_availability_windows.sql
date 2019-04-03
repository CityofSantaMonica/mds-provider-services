-- Windows of time a given provider's device was in the public right-of-way

DROP MATERIALIZED VIEW IF EXISTS csm_availability_windows CASCADE;

CREATE MATERIALIZED VIEW csm_availability_windows AS

WITH avail AS (
    SELECT
        provider_id,
        provider_name,
        vehicle_type,
        device_id,
        event_location,
        start_event_type,
        end_event_type,
        start_reason,
        end_reason,
        start_time,
        end_time
    FROM
        inactive_windows
    UNION
    SELECT
        provider_id,
        provider_name,
        vehicle_type,
        device_id,
        event_location,
        start_event_type,
        end_event_type,
        start_reason,
        end_reason,
        start_time,
        end_time
    FROM
        active_windows
)

SELECT
    provider_id,
    provider_name,
    device_id,
    vehicle_type,
    start_event_type,
    start_reason,
    end_event_type,
    end_reason,
    csm_local_timestamp(start_time) AS start_time_local,
    csm_local_timestamp(end_time) AS end_time_local,
    start_time,
    end_time,
    event_location
FROM
    avail
ORDER BY
    (provider_name, vehicle_type, start_time, end_time)

WITH NO DATA
;

CREATE INDEX csm_availability_windows_timestamp_idx
    ON csm_availability_windows (provider_name, vehicle_type, start_time, end_time desc)
;

CREATE INDEX csm_availability_windows_timestamp_local_idx
    ON csm_availability_windows (provider_name, vehicle_type, start_time_local, end_time_local desc)
;