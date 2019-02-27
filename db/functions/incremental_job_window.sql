/*
 * Given the name of a table tracked in 'jobs', calculate the beginning and end of the next processing window.
 */

CREATE OR REPLACE FUNCTION csm_incremental_job_window(job_name text, OUT window_start bigint, OUT window_end bigint)
RETURNS record
LANGUAGE plpgsql
AS $function$
DECLARE
    table_to_lock regclass;
BEGIN
    /*
     * Perform aggregation from the last processed ID + 1 up to the last committed ID.
     * SELECT .. FOR UPDATE on the row in the jobs table to prevent concurrent runs.
     */
    SELECT src_table, last_processed_id+1, pg_sequence_last_value(src_sequence)
    INTO table_to_lock, window_start, window_end
    FROM jobs
    WHERE name = job_name FOR UPDATE;

    IF NOT FOUND THEN
        RAISE 'job ''%'' is not in the jobs table', job_name;
    END IF;

    IF window_end IS NULL THEN
        /* sequence was never used */
        window_end := 0;
        RETURN;
    END IF;

    /*
     * Very briefly lock the table for writes in order to wait for all pending writes to finish.
     * This ensures there are no more uncommitted writes with an identifier lower or equal to window_end.
     * By throwing an exception, release the lock immediately after obtaining it such that writes can resume.
     */
    BEGIN
        EXECUTE format('LOCK %s IN EXCLUSIVE MODE', table_to_lock);
        RAISE 'release table lock';
    EXCEPTION WHEN OTHERS THEN
    END;

    /*
     * Remember the end of the window to continue next time.
     */
    UPDATE jobs SET last_processed_id = window_end WHERE name = job_name;
END;
$function$;