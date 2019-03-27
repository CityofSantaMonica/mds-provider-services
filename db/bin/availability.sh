#!/bin/bash
set -e

# setup the MDS availability views

if [[ "$1" == "refresh" ]]; then
    echo "refreshing availability"
    psql -v ON_ERROR_STOP=1 --host "$POSTGRES_HOSTNAME" --dbname "$MDS_DB" << EOSQL
    REFRESH MATERIALIZED VIEW device_event_timeline;
    REFRESH MATERIALIZED VIEW active_windows;
    REFRESH MATERIALIZED VIEW inactive_windows;
    REFRESH MATERIALIZED VIEW csm_availability;
EOSQL
else
    echo "rebuilding availability views"
    psql -v ON_ERROR_STOP=1 \
        --host "$POSTGRES_HOSTNAME" \
        --dbname "$MDS_DB" \
        --file availability/device_event_timeline.sql \
        --file availability/active_windows.sql \
        --file availability/inactive_windows.sql \
        --file availability/csm_availability.sql \
        --file availability/lost_devices.sql
fi
