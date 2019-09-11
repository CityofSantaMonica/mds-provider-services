BEGIN;

-- status_changes

DROP INDEX status_changes_sequence_id_idx;

ALTER SEQUENCE status_changes_sequence_id_seq
    RENAME TO status_changes_mds02x_sequence_id_seq;

ALTER TABLE status_changes
    DROP CONSTRAINT unique_event;

ALTER TABLE status_changes
    RENAME TO status_changes_mds02x;

CREATE TABLE status_changes (
    provider_id uuid not null,
    provider_name text not null,
    device_id uuid not null,
    vehicle_id text not null,
    vehicle_type vehicle_types not null,
    propulsion_type propulsion_types[] not null,
    event_type event_types not null,
    event_type_reason event_type_reasons not null,
    event_time timestamptz not null,
    publication_time timestamptz null,
    event_location jsonb not null,
    battery_pct double precision null,
    associated_trip uuid null,
    sequence_id bigserial not null,
    CONSTRAINT unique_event UNIQUE (provider_id, device_id, event_type, event_type_reason, event_time)
);

INSERT INTO status_changes (
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
    associated_trip,
    sequence_id
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
    event_location,
    battery_pct,
    associated_trips[1],
    sequence_id
FROM
    status_changes_mds02x
ORDER BY
    sequence_id
;

-- trips

DROP INDEX route_points_sequence_id_idx;

ALTER SEQUENCE route_points_sequence_id_seq
    RENAME TO route_points_mds02x_sequence_id_seq;

ALTER TABLE route_points
    DROP CONSTRAINT unique_route_point;

ALTER TABLE route_points
    RENAME TO route_points_mds02x;

DROP INDEX routes_sequence_id_idx;

ALTER SEQUENCE routes_sequence_id_seq
    RENAME TO routes_mds02x_sequence_id_seq;

ALTER TABLE routes
    DROP CONSTRAINT routes_pkey;

ALTER TABLE routes
    RENAME TO routes_mds02x;

CREATE TABLE routes (
    provider_id uuid,
    trip_id uuid,
    total_points int,
    in_csm_points int,
    in_dtsm_points int,
    route_line geometry,
    first_csm_geopoint geometry,
    last_csm_geopoint geometry,
    first_csm_timepoint timestamptz,
    last_csm_timepoint timestamptz,
    geopoints geometry[],
    timepoints timestamptz[],
    CONSTRAINT pk_routes PRIMARY KEY (provider_id, trip_id)
);

DROP INDEX trips_sequence_id_idx;

ALTER SEQUENCE trips_sequence_id_seq
    RENAME TO trips_mds02x_sequence_id_seq;

ALTER TABLE trips
    DROP CONSTRAINT pk_trips;

ALTER TABLE trips
    RENAME TO trips_mds02x;

CREATE TABLE trips (
    provider_id uuid not null,
    provider_name text not null,
    device_id uuid not null,
    vehicle_id text not null,
    vehicle_type vehicle_types not null,
    propulsion_type propulsion_types[] not null,
    trip_id uuid not null,
    trip_duration integer not null,
    trip_distance integer not null,
    route jsonb not null,
    accuracy integer not null,
    start_time timestamptz not null,
    end_time timestamptz not null,
    parking_verification_url text null,
    standard_cost integer null,
    actual_cost integer null,
    publication_time timestamptz null,
    sequence_id bigserial not null,
    CONSTRAINT pk_trips PRIMARY KEY (provider_id, trip_id)
);

CREATE OR REPLACE FUNCTION csm_to_timestamp(double precision)
    RETURNS timestamptz
    LANGUAGE sql
    STABLE
AS $BODY$
    SELECT
        CASE
            WHEN to_timestamp($1) > now() THEN
                to_timestamp($1 / 1000.0)
            ELSE
                to_timestamp($1)
        END;
$BODY$;

/* trigger function */
CREATE OR REPLACE FUNCTION csm_process_trip_route()
    RETURNS TRIGGER
    LANGUAGE plpgsql
AS $FUNCTION$
BEGIN
    WITH route_points AS (
        SELECT
            coords.f as feature,
            coords.ts as feature_timestamp,
            csm_parse_feature_geom(coords.f) as geopoint,
            csm_to_timestamp(coords.ts) as timepoint,
            st_contains(csm_city_boundary(), csm_parse_feature_geom(coords.f)) as in_csm,
            st_contains(csm_downtown_district(), csm_parse_feature_geom(coords.f)) as in_dtsm
        FROM (
            SELECT DISTINCT f, (f -> 'properties' ->> 'timestamp')::numeric as ts
            FROM jsonb_array_elements(new.route -> 'features') f
        ) coords
    )

    INSERT INTO routes (
        provider_id,
        trip_id,
        total_points,
        in_csm_points,
        in_dtsm_points,
        route_line,
        first_csm_geopoint,
        last_csm_geopoint,
        geopoints,
        first_csm_timepoint,
        last_csm_timepoint,
        timepoints
    )
    SELECT
        new.provider_id,
        new.trip_id,
        count(*) as total_points,
        count(*) filter (where in_csm) as in_csm_points,
        count(*) filter (where in_dtsm) as in_dtsm_points,
        st_makeline(array_agg(geopoint order by timepoint)) as route_line,
        ((array_agg(geopoint order by timepoint) filter (where in_csm)))[1] as first_csm_geopoint,
        ((array_agg(geopoint order by timepoint desc) filter (where in_csm)))[1] as last_csm_geopoint,
        array_agg(geopoint order by timepoint) as geopoints,
        min(timepoint) filter (where in_csm) as first_csm_timepoint,
        max(timepoint) filter (where in_csm) as last_csm_timepoint,
        array_agg(timepoint order by timepoint) as timepoints
    FROM
        route_points
    ON CONFLICT ON CONSTRAINT pk_routes DO UPDATE SET
        total_points = EXCLUDED.total_points,
        in_csm_points = EXCLUDED.in_csm_points,
        in_dtsm_points = EXCLUDED.in_dtsm_points,
        route_line = EXCLUDED.route_line,
        first_csm_geopoint = EXCLUDED.first_csm_geopoint,
        last_csm_geopoint = EXCLUDED.last_csm_geopoint,
        geopoints = EXCLUDED.geopoints,
        first_csm_timepoint = EXCLUDED.first_csm_timepoint,
        last_csm_timepoint = EXCLUDED.last_csm_timepoint,
        timepoints = EXCLUDED.timepoints;

    RETURN new;
END;
$FUNCTION$;

/* trigger */
DROP TRIGGER IF EXISTS process_inserted_trip ON trips CASCADE;

CREATE TRIGGER process_inserted_trip
     AFTER INSERT ON trips
     FOR EACH ROW
     EXECUTE PROCEDURE csm_process_trip_route();

INSERT INTO trips (
    provider_id,
    provider_name,
    device_id,
    vehicle_id,
    vehicle_type,
    propulsion_type,
    trip_id,
    trip_duration,
    trip_distance,
    route,
    accuracy,
    start_time,
    end_time,
    parking_verification_url,
    standard_cost,
    actual_cost,
    sequence_id
)
SELECT
    provider_id,
    provider_name,
    device_id,
    vehicle_id,
    vehicle_type,
    propulsion_type,
    trip_id,
    trip_duration,
    trip_distance,
    route,
    accuracy,
    start_time,
    end_time,
    parking_verification_url,
    standard_cost,
    actual_cost,
    sequence_id
FROM
    trips_mds02x
ORDER BY
    sequence_id
;

DROP FUNCTION IF EXISTS csm_classify_route_points();

DROP FUNCTION IF EXISTS csm_aggregate_routes();

DROP FUNCTION IF EXISTS csm_process_trip_routes();

DROP FUNCTION IF EXISTS csm_incremental_job_window();

DROP TABLE IF EXISTS jobs CASCADE;

INSERT INTO migrations (version, date)
VALUES ('mds-0.3.x', now());

COMMIT;