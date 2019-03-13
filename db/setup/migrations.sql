/*
 * Tracking the version update history.
 */

DROP TABLE IF EXISTS migrations CASCADE;

CREATE TABLE migrations (
    version text primary key,
    date timestamptz not null,
    sequence_id bigserial not null
);