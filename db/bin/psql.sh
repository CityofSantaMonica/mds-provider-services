#!/bin/bash
set -e

export PGUSER=$MDS_USER
export PGPASSWORD=$MDS_PASSWORD

args="$@"

psql -v ON_ERROR_STOP=1 \
    --host "$POSTGRES_HOSTNAME" \
    --dbname "$MDS_DB" \
    "$args"