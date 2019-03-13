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
    event_location jsonb not null,
    battery_pct double precision,
    associated_trips uuid[],
    CONSTRAINT unique_event UNIQUE (provider_id, device_id, event_type, event_type_reason, event_time)
);

CREATE INDEX status_changes_sequence_id_idx
    ON status_changes USING brin (sequence_id)
;