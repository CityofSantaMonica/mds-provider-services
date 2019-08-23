BEGIN;

DROP TABLE IF EXISTS route_points CASCADE;

DROP TABLE IF EXISTS routes CASCADE;

DROP TABLE IF EXISTS jobs;

DROP FUNCTION IF EXISTS csm_classify_route_points() CASCADE;

DROP FUNCTION IF EXISTS csm_incremental_job_window() CASCADE;

DROP INDEX status_changes_sequence_id_idx;

DROP INDEX trips_sequence_id_idx;

INSERT INTO migrations (version, date)
VALUES ('0.6.0', now())

COMMIT;