DROP TABLE IF EXISTS status_changes CASCADE;

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
