#! /bin/bash

docker-compose up --detach client server &&

docker-compose run --rm db reset &&
docker-compose run --rm db init