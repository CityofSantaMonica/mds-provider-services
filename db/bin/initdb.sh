#!/bin/bash
set -e

# run the MDS setup scripts

psql -v ON_ERROR_STOP=1 \
    --host "$POSTGRES_HOSTNAME" \
    --dbname "$MDS_DB" \
    --file setup/enums.sql \
    --file setup/trips.sql \
    --file setup/status_changes.sql \
    --file setup/vehicles.sql \
    --file setup/migrations.sql
