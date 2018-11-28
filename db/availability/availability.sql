CREATE MATERIALIZED VIEW public.v_availability AS

WITH full_table AS (
    WITH cte AS (
        WITH no_repeat_table AS (
            WITH repeat_table AS (
                WITH row_table AS (
                    -- add a row number to each status_change row
                    -- events ordered chronologically for each provider's device
                    SELECT
                        *,
                        row_number() OVER (ORDER BY provider_name, vehicle_type, device_id, event_time) AS row_num
                    FROM
                        status_changes
                ) -- row_table
                SELECT
                    _left.provider_name,
                    _left.device_id,
                    _left.vehicle_id,
                    _left.vehicle_type,
                    _left.propulsion_type,
                    _left.event_type,
                    _left.event_type_reason,
                    _left.event_time,
                    _left.event_location,
                    _left.battery_pct,
                    _left.associated_trips,
                    _left.row_num,
                    _right.row_num IS NULL AS condition
                FROM
                    row_table _left LEFT JOIN row_table _right
                    ON _left.provider_id = _right.provider_id
                    AND _left.device_id = _right.device_id
                    AND (_left.row_num + 1) = _right.row_num
                    AND ((_left.event_type = 'available'::event_types AND _right.event_type = 'available'::event_types) OR
                         (_left.event_type <> 'available'::event_types AND _right.event_type <> 'available'::event_types))
            ) -- repeat_table
            SELECT
                repeat_table.provider_name,
                repeat_table.vehicle_type,
                repeat_table.propulsion_type,
                repeat_table.device_id,
                repeat_table.vehicle_id,
                repeat_table.event_type,
                repeat_table.event_type_reason,
                repeat_table.event_time,
                repeat_table.event_location,
                repeat_table.battery_pct,
                repeat_table.associated_trips
            FROM
                repeat_table
            WHERE
                repeat_table.condition
        ) -- no_repeat_table
        SELECT
            no_repeat_table.provider_name,
            no_repeat_table.vehicle_type,
            no_repeat_table.propulsion_type,
            no_repeat_table.device_id,
            no_repeat_table.vehicle_id,
            no_repeat_table.event_type,
            no_repeat_table.event_type_reason,
            no_repeat_table.event_time,
            no_repeat_table.event_location,
            no_repeat_table.battery_pct,
            no_repeat_table.associated_trips,
            row_number() OVER () AS n
        FROM
            no_repeat_table
    ), -- cte
    trip_rows AS (
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
            event_location,
            battery_pct,
            associated_trips,
            row_number() OVER (ORDER BY provider_name, vehicle_type, device_id, event_time) AS row_num
        FROM
            status_changes
    ) -- trip_rows
    SELECT
        avail.provider_name,
        avail.vehicle_type,
        avail.propulsion_type,
        avail.device_id,
        avail.vehicle_id,
        avail.event_location AS event_location,
        avail.event_type AS start_event_type,
        notavail.event_type AS end_event_type,
        avail.event_type_reason AS start_reason,
        notavail.event_type_reason AS end_reason,
        avail.event_time AS start_time,
        notavail.event_time AS end_time
    FROM
        cte avail LEFT JOIN cte notavail
        ON avail.event_type = 'available'::event_types
        AND notavail.event_type <> 'available'::event_types
        AND avail.device_id = notavail.device_id
        AND (avail.n + 1) = notavail.n
    WHERE
        avail.event_type = 'available'::event_types

    UNION

    SELECT
        userpkup.provider_name,
        userpkup.vehicle_type,
        userpkup.propulsion_type,
        userpkup.device_id,
        userpkup.vehicle_id,
        userpkup.event_location AS event_location,
        userpkup.event_type AS start_event_type,
        next_event.event_type AS end_event_type,
        userpkup.event_type_reason AS start_reason,
        next_event.event_type_reason AS end_reason,
        userpkup.event_time AS start_time,
        next_event.event_time AS end_time
    FROM
        trip_rows userpkup LEFT JOIN trip_rows next_event
        ON userpkup.event_type_reason = 'user_pick_up'::event_type_reasons
        AND userpkup.device_id = next_event.device_id
        AND (userpkup.row_num + 1) = next_event.row_num
    WHERE
        userpkup.event_type_reason = 'user_pick_up'::event_type_reasons
) -- full_table
SELECT
    provider_name,
    vehicle_type,
    vehicle_id,
    csm_local_timestamp(start_time) AS start_time_local,
    csm_local_timestamp(end_time) AS end_time_local,
    (end_time - start_time) as delta_time,
    EXTRACT(EPOCH FROM (end_time - start_time)) as delta_seconds,
    round(EXTRACT(EPOCH FROM (end_time - start_time))::numeric / 3600, 2) as delta_hours,
    round(EXTRACT(EPOCH FROM (end_time - start_time))::numeric / 86400, 1) as delta_days,
    start_event_type,
    start_reason,
    end_event_type,
    end_reason,
    csm_parse_feature_geom(event_location) AS event_location,
    propulsion_type,
    device_id,
    start_time,
    end_time
FROM
    full_table
ORDER BY
    (start_time, provider_name, vehicle_type, device_id)

WITH NO DATA;