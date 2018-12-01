#!/bin/sh
set -e

# run a migration script by version number

export PGUSER=$MDS_USER
export PGPASSWORD=$MDS_PASSWORD

file="migrate/$1.sql"

if [ -e "$file" ]; then
    psql \
        --host "$POSTGRES_HOSTNAME" \
        --dbname "$MDS_DB" \
        --file "$file"
fi
