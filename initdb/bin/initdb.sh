#!/bin/sh
set -e

# run the MDS setup scripts

export PGUSER=$MDS_USER
export PGPASSWORD=$MDS_PASSWORD

psql \
    --host "$POSTGRES_HOSTNAME" \
    --dbname "$MDS_DB" \
    --file scripts/common.sql \
    --file scripts/trips.sql \
    --file scripts/status_changes.sql
