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

Run the setup scripts from within the running container directly, or by
using the container in executable form with Compose.

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

Run a [migration](migrate/) script with the given version number.

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
