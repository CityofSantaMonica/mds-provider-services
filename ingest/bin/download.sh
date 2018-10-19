#! /bin/bash

docker-compose run --rm ingest \
    --no_load \
    --output "$@"