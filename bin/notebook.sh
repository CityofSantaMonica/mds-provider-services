#! /bin/bash

docker-compose run --service-ports --entrypoint bash "$1" start-notebook.sh