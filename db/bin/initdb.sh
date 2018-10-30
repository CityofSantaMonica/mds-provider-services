#!/bin/sh
set -e

# run the MDS setup scripts

export PGUSER=$MDS_USER
export PGPASSWORD=$MDS_PASSWORD

psql \
    --host "$POSTGRES_HOSTNAME" \
    --dbname "$MDS_DB" \
    --file scripts/setup/common.sql \
    --file scripts/setup/trips.sql \
    --file scripts/setup/status_changes.sql
