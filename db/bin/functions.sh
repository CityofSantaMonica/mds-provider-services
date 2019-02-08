#!/bin/bash
set -e

# setup MDS related functions

export PGUSER=$MDS_USER
export PGPASSWORD=$MDS_PASSWORD

for file in functions/*.sql; do
    psql -v ON_ERROR_STOP=1 \
    --host "$POSTGRES_HOSTNAME" \
    --dbname "$MDS_DB" \
    --file "$file"
done
