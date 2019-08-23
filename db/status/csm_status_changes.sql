DROP VIEW IF EXISTS csm_status_changes CASCADE;

CREATE VIEW csm_status_changes AS

SELECT
    provider_id,
    provider_name,
    device_id,
    vehicle_id,
    vehicle_type,
    propulsion_type,
    event_type,
    event_type_reason,
    event_type = 'available'::event_types AND event_type_reason <> 'user_drop_off'::event_type_reasons AS deployment,
    event_type_reason = 'user_drop_off'::event_type_reasons AS user_drop_off,
    event_time,
    csm_local_timestamp(event_time) AS event_time_local,
    csm_parse_feature_geom(event_location) AS event_geom,
    st_contains(csm_downtown_district(), csm_parse_feature_geom(event_location)) AS downtown,
    st_x(csm_parse_feature_geom(event_location)) AS event_lon,
    st_y(csm_parse_feature_geom(event_location)) AS event_lat,
    event_location,
    battery_pct,
    publication_time,
    csm_local_timestamp(publication_time) as publication_time_local,
    associated_trip
FROM
    status_changes
WHERE
    st_contains(csm_city_boundary(), csm_parse_feature_geom(event_location));
