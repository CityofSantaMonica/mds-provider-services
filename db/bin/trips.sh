#!/bin/bash
set -e

# setup the MDS trip route tables and processing jobs

export PGUSER=$MDS_USER
export PGPASSWORD=$MDS_PASSWORD

if [[ "$1" == "refresh" ]]; then
    echo "refreshing trips"
    psql -v ON_ERROR_STOP=1 --host "$POSTGRES_HOSTNAME" --dbname "$MDS_DB" << EOSQL
    REFRESH MATERIALIZED VIEW csm_trips;
EOSQL
else
    echo "rebuilding trip route tables"
    psql -v ON_ERROR_STOP=1 \
        --host "$POSTGRES_HOSTNAME" \
        --dbname "$MDS_DB" \
        --file trips/process_trip_routes.sql \
        --file trips/csm_trips.sql
fi
