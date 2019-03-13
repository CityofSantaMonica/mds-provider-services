# db

Work with a MDS `provider` Postgres database.

## Configuration

This container uses the following [environment variables][env] to connect to the MDS database:

```bash
POSTGRES_HOSTNAME=server
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres_password

MDS_DB=mds_provider
MDS_USER=mds_provider
MDS_PASSWORD=mds_provider_password
```

## Container commands

This container comes with a number of sub-commands for interacting with a database using the configured environment.

### Initialize the database from scratch

```bash
docker-compose run db reset

docker-compose run db init
```

### Functions

Setup postgres [functions](functions/) used in other views and calculations.

```bash
docker-compose run db functions
```

### Migrations

Run a [migration](migrations/) script with the given version number.

```bash
docker-compose run db migrate VERSION
```

Where `VERSION` is a version number like `x.y.z`.

### Availability

Create the [`availability`](availability/) view and associated infrastructure.

[Trips](#trips) must be configured first.

#### Run the intialization scripts

```bash
docker-compose run db availability
```

#### Refresh the materialized views

```bash
docker-compose run db availability refresh
```

### Deployments

Create the [`deployments`](deployments/) views.

#### Run the intialization scripts

```bash
docker-compose run db deployments
```

#### Refresh the materialized view

From the current contents of the `status_changes` table.

```bash
docker-compose run db deployments refresh
```

### Trips

Create additional [`trips`](trips/) and routes views.

#### Run the intialization scripts

```bash
docker-compose run db trips
```

#### Run the incremental processing job

```bash
docker-compose run db trips refresh
```

### `psql`

Pass through to [`psql`][psql] running in the container against `$MDS_DB` as `$MDS_USER`.

#### Interactive prompt

```bash
$ docker-compose run db psql

server is available
psql <version info>
Type "help" for help.

mds_provider=> help

You are using psql, the command-line interface to PostgreSQL.
Type:  \copyright for distribution terms
       \h for help with SQL commands
       \? for help with psql commands
       \g or terminate with semicolon to execute query
       \q to quit

mds_provider=>
```

#### Query/command

```bash
$ docker-compose run db psql -c "select distinct provider_name from trips;"

server is available
 provider_name
---------------
 bird
 Lime
 JUMP
 Lyft
(4 rows)
```


[env]: https://github.com/CityofSantaMonica/mds-provider-services#1-create-an-env-file
[psql]: https://www.postgresql.org/docs/current/app-psql.html