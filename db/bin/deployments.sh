#!/bin/bash
set -e

# setup the MDS deployments views

export PGUSER=$MDS_USER
export PGPASSWORD=$MDS_PASSWORD

if [[ "$1" == "refresh" ]]; then
    echo "refreshing csm_deployments"
    psql -v ON_ERROR_STOP=1 --host "$POSTGRES_HOSTNAME" --dbname "$MDS_DB" << EOSQL
    REFRESH MATERIALIZED VIEW public.csm_deployments;
EOSQL
else
    psql -v ON_ERROR_STOP=1 \
        --host "$POSTGRES_HOSTNAME" \
        --dbname "$MDS_DB" \
        --file deployments/deployments.sql \
        --file deployments/daily.sql \
        --file deployments/weekly.sql
fi
