#!/bin/bash
set -e

# wait for the postgres server to be available before executing cmd
# attempt to dispatch to some know sub-commands

cmd="$@"
sub="$1"
shift

until PGPASSWORD=$POSTGRES_PASSWORD PGUSER=$POSTGRES_USER \
      psql -h "$POSTGRES_HOSTNAME" -d "$POSTGRES_DB" -c '\q'; do
  >&2 echo "Waiting for $POSTGRES_HOSTNAME"
  sleep 3
done

>&2 echo "$POSTGRES_HOSTNAME is available"

case $sub in
    avail|availability) cmd="bin/availability.sh" ;;
    deployments) cmd="bin/deployments.sh" ;;
    file) cmd="psql -v ON_ERROR_STOP=1 --host ${POSTGRES_HOSTNAME} --dbname ${MDS_DB} --file" ;;
    functions) cmd="bin/functions.sh" ;;
    init) cmd="bin/initdb.sh" ;;
    migrate|migrations) cmd="bin/migrations.sh" ;;
    psql) cmd="psql -v ON_ERROR_STOP=1 --host ${POSTGRES_HOSTNAME} --dbname ${MDS_DB}" ;;
    query) cmd="psql -v ON_ERROR_STOP=1 --host ${POSTGRES_HOSTNAME} --dbname ${MDS_DB} --command" ;;
    reset) cmd="bin/reset.sh" ;;
    routes|trips) cmd="bin/trips.sh" ;;
esac

export PGUSER=$MDS_USER
export PGPASSWORD=$MDS_PASSWORD

exec $cmd "$@"
