CREATE TABLE migrations (
    version text primary key,
    date timestamptz not null,
    sequence_id bigserial not null
);

CREATE TABLE jobs (
    name text primary key,
    src_table text not null,
    src_sequence text not null,
    last_processed_id bigint default 0
);

ALTER TABLE status_changes
    RENAME TO status_changes_030
;

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
    event_location jsonb not null,
    battery_pct double precision,
    associated_trips uuid[],
    sequence_id bigserial not null,
    CONSTRAINT unique_event UNIQUE (provider_id, device_id, event_type, event_type_reason, event_time)
);

CREATE INDEX status_changes_sequence_id_idx
    ON status_changes USING brin (sequence_id)
;

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
    associated_trips
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
    associated_trips
FROM
    status_changes_030
;

ALTER TABLE trips
    RENAME TO trips_030
;

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
    parking_verification_url text,
    standard_cost integer,
    actual_cost integer,
    sequence_id bigserial not null,
    CONSTRAINT pk_trips PRIMARY KEY (provider_id, trip_id)
);

CREATE INDEX trips_sequence_id_idx
    ON trips USING brin (sequence_id)
;

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
    actual_cost
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
    actual_cost
FROM
    trips_030
;

INSERT INTO migrations (version, date)
VALUES ('0.4.0', now())
;