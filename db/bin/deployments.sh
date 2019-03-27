#!/bin/bash
set -e

# setup the MDS deployments views

echo "rebuilding deployment views"

psql -v ON_ERROR_STOP=1 \
    --host "$POSTGRES_HOSTNAME" \
    --dbname "$MDS_DB" \
    --file deployments/deployments.sql \
    --file deployments/daily.sql \
    --file deployments/devices_7_days.sql \
    --file deployments/devices_31_days.sql \
    --file deployments/devices_6_months.sql
