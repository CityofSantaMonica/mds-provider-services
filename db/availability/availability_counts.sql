DROP TABLE IF EXISTS availability_counts CASCADE;

CREATE TABLE IF NOT EXISTS availability_counts (
    provider_name text not null,
    vehicle_type vehicle_types not null,
    start_time timestamptz not null,
    end_time timestamptz not null,
    avg_availability double precision not null,
    cutoff int not null,
    CONSTRAINT unique_count UNIQUE (provider_name, vehicle_type, start_time, end_time, cutoff)
);