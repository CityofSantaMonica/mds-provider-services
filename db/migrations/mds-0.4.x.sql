ALTER TYPE vehicle_types ADD VALUE 'moped';
ALTER TYPE vehicle_types ADD VALUE 'car';

BEGIN;

ALTER TABLE status_changes
    ADD COLUMN associated_ticket text null;

ALTER TABLE trips
    ADD COLUMN currency character(3) null default 'USD';

CREATE TABLE vehicles (
    provider_id uuid not null,
    provider_name text not null,
    device_id uuid not null,
    vehicle_id text not null,
    vehicle_type vehicle_types not null,
    propulsion_type propulsion_types[] not null,
    last_event_time timestamptz not null,
    last_event_type event_types not null,
    last_event_type_reason event_type_reasons not null,
    last_event_location jsonb not null,
    current_location jsonb null,
    battery_pct double precision null,
    last_updated timestamptz not null,
    ttl integer not null,
    sequence_id bigserial not null,
    CONSTRAINT unique_vehicle_event UNIQUE (provider_id, device_id, last_updated)
);


INSERT INTO migrations (version, date)
VALUES ('mds-0.4.x', now());

COMMIT;