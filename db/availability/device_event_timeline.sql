-- A de-deduplicated device event timeline

DROP MATERIALIZED VIEW IF EXISTS device_event_timeline CASCADE;

CREATE MATERIALIZED VIEW device_event_timeline AS

WITH timeline_dupes AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY provider_id, vehicle_type, device_id ORDER BY event_time) AS row_num
    FROM
        status_changes
    WHERE
        st_contains(csm_city_boundary(), csm_parse_feature_geom(event_location))
    ORDER BY
        provider_id,
        vehicle_type,
        row_num
)

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
    csm_parse_feature_geom(event_location) AS event_location,
    battery_pct,
    associated_trips,
    row_number() OVER (PARTITION BY provider_id, vehicle_type, device_id ORDER BY event_time) AS row_num
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
                timeline_dupes _left LEFT JOIN timeline_dupes _right
                ON _left.provider_id = _right.provider_id
                AND _left.vehicle_type = _right.vehicle_type
                AND _left.device_id = _right.device_id
                -- the next event in the timeline for this provider's device
                AND (_left.row_num + 1) = _right.row_num
                -- two consecutive, both'available'
                AND ((_left.event_type = 'available'::event_types AND _right.event_type = 'available'::event_types) OR
                -- two consecutive, neither 'available'
                (_left.event_type <> 'available'::event_types AND _right.event_type <> 'available'::event_types))
        ) dupe
        WHERE
            dupe.condition
    ) no_dupe

WITH NO DATA
;