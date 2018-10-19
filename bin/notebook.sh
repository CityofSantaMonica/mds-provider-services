#! /bin/bash

docker-compose run --rm --service-ports --entrypoint bash "$1" start-notebook.sh