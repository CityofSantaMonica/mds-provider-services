-- A de-deduplicated device event timeline

CREATE VIEW public.device_event_timeline_dedupe AS

SELECT
    *,
    row_number() OVER () AS row_num
FROM
    (SELECT -- the non-duplicated records
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
        associated_trips
        FROM
            (SELECT -- the duplicate records
                _left.provider_id,
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
                -- join with self, for each provider's device
                device_event_timeline _left LEFT JOIN device_event_timeline _right
                ON _left.provider_id = _right.provider_id
                AND _left.device_id = _right.device_id
                -- the next row, e.g. the next event in the timeline for this provider's device
                AND (_left.row_num + 1) = _right.row_num
                -- both 'available' -> user/data/transmission error
                AND ((_left.event_type = 'available'::event_types AND _right.event_type = 'available'::event_types) OR
                -- both the same, not 'avail' -> 
                    (_left.event_type <> 'available'::event_types AND _right.event_type <> 'available'::event_types))
        ) dupe
        WHERE
            dupe.condition
    ) no_dupe

;