#!/bin/bash
set -e

# setup the MDS availability views

export PGUSER=$MDS_USER
export PGPASSWORD=$MDS_PASSWORD

if [[ "$1" == "refresh" ]]; then
    echo "refreshing csm_availability"
    psql -v ON_ERROR_STOP=1 --host "$POSTGRES_HOSTNAME" --dbname "$MDS_DB" << EOSQL
    REFRESH MATERIALIZED VIEW public.csm_availability;
EOSQL
else
    psql -v ON_ERROR_STOP=1 \
        --host "$POSTGRES_HOSTNAME" \
        --dbname "$MDS_DB" \
        --file availability/device_event_timeline.sql \
        --file availability/device_event_timeline_dedupe.sql \
        --file availability/available_windows.sql \
        --file availability/trip_windows.sql \
        --file availability/availability.sql \
        --file availability/csm.sql
fi
