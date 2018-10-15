#! /bin/bash

docker-compose run ingest \
    --end_time `date +%Y%m%dT%H0000%z` \
    --duration 3600 \
    "$@"