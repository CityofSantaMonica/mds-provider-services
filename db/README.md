# db

Work with a MDS `provider` Postgres database.

## Configuration

This container uses the following environment variables to connect to the MDS database:

```bash
POSTGRES_HOSTNAME=server
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres_password

MDS_DB=mds_provider
MDS_USER=mds_provider
MDS_PASSWORD=mds_provider_password
```

## Setup scripts

Run the [setup scripts](bin/) from within the running container directly, or by
using the container in executable form with Compose.

### Initialize the database

Run by default when the container starts up.

```console
docker-compose run db bin/initdb.sh
```

### Reset the database

Tears down the MDS database and then re-initializes.

```console
docker-compose run db bin/reset.sh
```
