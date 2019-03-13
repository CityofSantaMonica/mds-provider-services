#!/bin/bash
set -e

# run a migration script by version number

export PGUSER=$MDS_USER
export PGPASSWORD=$MDS_PASSWORD

file="migrations/$1.sql"

if [[ -e "$file" ]]; then
    psql -v ON_ERROR_STOP=1 \
        --host "$POSTGRES_HOSTNAME" \
        --dbname "$MDS_DB" \
        --file "$file"
fi
