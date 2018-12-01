#!/bin/sh
set -e

# setup the MDS availability view

export PGUSER=$MDS_USER
export PGPASSWORD=$MDS_PASSWORD

if [ "$1" == "reset" && ! -z "$2" ]; then
    psql -v ON_ERROR_STOP=1 --host "$POSTGRES_HOSTNAME" --dbname "$POSTGRES_DB" <<-EOSQL
        DROP MATERIALIZED VIEW IF EXISTS $2 CASCADE;
EOSQL

elif [ "$1" == "refresh" ]; then
    psql -v ON_ERROR_STOP=1 --host "$POSTGRES_HOSTNAME" --dbname "$MDS_DB" <<-EOSQL
        "REFRESH MATERIALIZED VIEW availability;"
EOSQL

else
    psql \
        --host "$POSTGRES_HOSTNAME" \
        --dbname "$MDS_DB" \
        --file functions/city_boundary.sql \
        --file functions/downtown_district.sql \
        --file functions/local_timestamp.sql \
        --file parse_feature_geom.sql \
        --file availability/device_event_timeline.sql \
        --file availability/device_event_timeline_dedupe.sql \
        --file availability/available_windows.sql \
        --file availability/trip_windows.sql \
        --file availability/window_unions.sql \
        --file availability/availability.sql
fi
