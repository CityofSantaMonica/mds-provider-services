#!/bin/bash
set -e

# setup the MDS route and trip views

export PGUSER=$MDS_USER
export PGPASSWORD=$MDS_PASSWORD

if [[ "$1" == "refresh" ]]; then
    echo "refreshing route_points and csm_routes"
    psql -v ON_ERROR_STOP=1 --host "$POSTGRES_HOSTNAME" --dbname "$MDS_DB" << EOSQL
    REFRESH MATERIALIZED VIEW public.route_points;
    REFRESH MATERIALIZED VIEW public.csm_routes;
EOSQL
else
    echo "rebuilding trips"
    psql -v ON_ERROR_STOP=1 \
        --host "$POSTGRES_HOSTNAME" \
        --dbname "$MDS_DB" \
        --file trips/route_points.sql \
        --file trips/routes.sql \
        --file trips/csm.sql
fi
