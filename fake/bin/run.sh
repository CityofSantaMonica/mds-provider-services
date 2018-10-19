#! /bin/bash

docker-compose build --no-cache fake

docker-compose run --rm fake "$@"