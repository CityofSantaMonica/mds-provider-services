/*
 * Jobs are incremental processing tasks on a source table.
 * The source table must have an associated sequence in order to support incremental processing.
 */

DROP TABLE IF EXISTS jobs CASCADE;

CREATE TABLE jobs (
    name text primary key,
    src_table text not null,
    src_sequence text not null,
    last_processed_id bigint default 0
);