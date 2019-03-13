DROP TABLE IF EXISTS trips CASCADE;

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