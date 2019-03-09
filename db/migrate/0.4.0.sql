ALTER TABLE status_changes
    RENAME TO status_changes_030
;

CREATE TABLE status_changes (
    provider_id uuid NOT NULL,
    provider_name text NOT NULL,
    device_id uuid NOT NULL,
    vehicle_id text NOT NULL,
    vehicle_type vehicle_types NOT NULL,
    propulsion_type propulsion_types[] NOT NULL,
    event_type event_types NOT NULL,
    event_type_reason event_type_reasons NOT NULL,
    event_time timestamp with time zone NOT NULL,
    event_location jsonb NOT NULL,
    battery_pct double precision,
    associated_trips uuid[],
    sequence_id bigserial NOT NULL,
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
    provider_id uuid NOT NULL,
    provider_name text NOT NULL,
    device_id uuid NOT NULL,
    vehicle_id text NOT NULL,
    vehicle_type vehicle_types NOT NULL,
    propulsion_type propulsion_types[] NOT NULL,
    trip_id uuid NOT NULL,
    trip_duration integer NOT NULL,
    trip_distance integer NOT NULL,
    route jsonb NOT NULL,
    accuracy integer NOT NULL,
    start_time timestamp with time zone NOT NULL,
    end_time timestamp with time zone NOT NULL,
    parking_verification_url text,
    standard_cost integer,
    actual_cost integer,
    sequence_id bigserial NOT NULL,
    primary key (provider_id, trip_id)
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