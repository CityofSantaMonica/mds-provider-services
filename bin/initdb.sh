#! /bin/bash

docker-compose run --rm db reset &&
docker-compose run --rm db init &&
docker-compose run --rm db functions &&
docker-compose run --rm db trips &&
docker-compose run --rm db availability