#!/bin/sh
set -e

# run the MDS setup scripts

export PGUSER=$MDS_USER
export PGPASSWORD=$MDS_PASSWORD

psql \
    --host "$POSTGRES_HOSTNAME" \
    --dbname "$MDS_DB" \
    --file setup/common.sql \
    --file setup/trips.sql \
    --file setup/status_changes.sql \

for filename in functions/*.sql; do
    psql \
        --host "$POSTGRES_HOSTNAME" \
        --dbname "$MDS_DB" \
        --file "$filename"
done
