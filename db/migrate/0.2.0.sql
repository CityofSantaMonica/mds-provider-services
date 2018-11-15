ALTER TABLE status_changes
    ALTER COLUMN event_location
    SET DATA TYPE jsonb
    USING event_location::jsonb;

ALTER TABLE status_changes
    DROP CONSTRAINT unique_event;

ALTER TABLE status_changes
    ADD CONSTRAINT unique_provider_device_event
    UNIQUE (provider_id, device_id, event_time);

ALTER TABLE trips
    ALTER COLUMN route
    SET DATA TYPE jsonb
    USING route::jsonb;