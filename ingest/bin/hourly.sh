#! /bin/bash

docker-compose run --rm --name "mds_provider_ingest_hourly_$RANDOM" ingest \
    --end_time `date +%Y%m%dT%H0000%z` \
    --duration 3600 \
    "$@"