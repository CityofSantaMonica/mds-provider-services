#!/bin/bash
set -e

# setup MDS status_changes-based views

echo "rebuilding status views"

psql -v ON_ERROR_STOP=1 \
    --host "$POSTGRES_HOSTNAME" \
    --dbname "$MDS_DB" \
    --file status/csm_status_changes.sql \
    --file status/deployments.sql \
    --file status/deployments_daily.sql
