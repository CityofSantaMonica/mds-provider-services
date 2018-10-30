ALTER TABLE status_changes
    ALTER COLUMN event_location
    SET DATA TYPE jsonb
    USING event_location::jsonb;

ALTER TABLE trips
    ALTER COLUMN route
    SET DATA TYPE jsonb
    USING route::jsonb;