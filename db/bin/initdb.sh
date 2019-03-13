#!/bin/bash
set -e

# run the MDS setup scripts

export PGUSER=$MDS_USER
export PGPASSWORD=$MDS_PASSWORD

psql -v ON_ERROR_STOP=1 \
    --host "$POSTGRES_HOSTNAME" \
    --dbname "$MDS_DB" \
    --file setup/enums.sql \
    --file setup/trips.sql \
    --file setup/status_changes.sql \
    --file setup/jobs.sql \
    --file setup/migrations.sql
