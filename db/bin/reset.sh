#!/bin/bash
set -e

# delete the MDS user and database
# recreate the MDS user and database
# enable postgis on the MDS database

# adding PGUSER and PGPASS
export PGUSER=$POSTGRES_USER
export PGPASSWORD=$POSTGRES_PASSWORD

psql -v ON_ERROR_STOP=1 --host "$POSTGRES_HOSTNAME" --dbname "$POSTGRES_DB" << EOSQL
    DROP DATABASE IF EXISTS $MDS_DB;

    DROP USER IF EXISTS $MDS_USER;

    CREATE USER $MDS_USER WITH PASSWORD '$MDS_PASSWORD';

    GRANT $MDS_USER TO $PGUSER;

    CREATE DATABASE $MDS_DB
        WITH OWNER $MDS_USER
        ENCODING 'UTF8'
        CONNECTION LIMIT -1
    ;

    
EOSQL

psql -v ON_ERROR_STOP=1 --host "$POSTGRES_HOSTNAME" --dbname "$MDS_DB" << EOSQL
    CREATE EXTENSION IF NOT EXISTS postgis;
EOSQL
