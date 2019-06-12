#!/bin/bash
set -e

# setup the MDS trip route tables and processing jobs

export PGUSER=$MDS_USER
export PGPASSWORD=$MDS_PASSWORD

if [[ "$1" == "refresh" ]]; then
    echo "refreshing trip routes"
    psql -v ON_ERROR_STOP=1 --host "$POSTGRES_HOSTNAME" --dbname "$MDS_DB" << EOSQL
    SELECT * FROM csm_process_trip_routes();
    REFRESH MATERIALIZED VIEW csm_trips;
EOSQL
else
    echo "rebuilding trip route tables"
    psql -v ON_ERROR_STOP=1 \
        --host "$POSTGRES_HOSTNAME" \
        --dbname "$MDS_DB" \
        --file trips/route_points.sql \
        --file trips/routes.sql \
        --file trips/process_trip_routes.sql \
        --file trips/csm_trips.sql \
        --file trips/daily.sql
fi
