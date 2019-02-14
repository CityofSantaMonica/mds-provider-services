#! /bin/bash

here=`dirname $0`
service="$1"
shift
args="$@"
notebook="jupyter notebook . --allow-root --ip=0.0.0.0 --port=8888 --no-browser"

$here/bash.sh --service-ports "$service" "-c" "$notebook $args"