#! /bin/bash

docker-compose run --rm ingest \
    --end_time `date +%Y%m%dT%H0000%z` \
    --duration 3600 \
    "$@"