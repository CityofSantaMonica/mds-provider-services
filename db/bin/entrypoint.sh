#!/bin/bash
set -e

# wait for the postgres server to be available before executing cmd
# attempt to dispatch to some know sub-commands

cmd="$@"
sub="$1"
shift
args="$@"

until PGPASSWORD=$POSTGRES_PASSWORD PGUSER=$POSTGRES_USER \
      psql -h "$POSTGRES_HOSTNAME" -d "$POSTGRES_DB" -c '\q'; do
  >&2 echo "Waiting for $POSTGRES_HOSTNAME"
  sleep 3
done

>&2 echo "$POSTGRES_HOSTNAME is available"

case $sub in
    avail|availability) cmd="bin/availability.sh $args" ;;
    deployments) cmd="bin/deployments.sh $args" ;;
    functions) cmd="bin/functions.sh $args" ;;
    init) cmd="bin/initdb.sh $args" ;;
    migrate|migrations) cmd="bin/migrations.sh $args" ;;
    psql) cmd="bin/psql.sh $args" ;;
    reset) cmd="bin/reset.sh $args" ;;
    routes|trips) cmd="bin/trips.sh $args" ;;
esac

exec $cmd
