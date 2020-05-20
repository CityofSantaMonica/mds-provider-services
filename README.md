# mds-provider-services

Services for working with [MDS `provider`][provider] data, built as runnable Docker containers.

These services are implemented via [`mds-provider`](https://github.com/CityofSantaMonica/mds-provider),
a general-purpose Python library for working with MDS Provider data.

## Batteries Included

The services are organized around specific functions. More detailed explanation can be found in service `README` files.

| service | description |
| --------- | ----------- |
| [`analytics`](analytics/) | Perform analysis on `provider` data |
| [`client`](#pgadmin-client) | [pgAdmin4][pgadmin] web client |
| [`db`](db/) | Work with a `provider` database |
| [`fake`](fake/) | Generate fake `provider` data for testing and development |
| [`ingest`](ingest/) | Ingest `provider` data from different sources |
| [`server`](#local-postgres-server) | Local [postgres][postgres] database server |
| [`validate`](ingest/README.md#validation) | Validate `provider` data feeds and/or local MDS payload files. |

## Getting Started

Requires both [Docker][docker] and [Docker Compose][compose].

Commands below should be run from the root of this repository, where the `docker-compose.yml` file lives.

### 0. Create a `docker-compose.yml` file

Copy the `dev` file and edit as necessary. Compose automatically uses this file for service definitions and configuration.

You shouldn't have to make too many (if any) changes; see the next step for environment variable configuration.

```bash
cp docker-compose.dev.yml docker-compose.yml
```

Alternatively, use the `dev` file as-is by prepending a switch to `docker-compose` commands, e.g.:

```bash
docker-compose -f docker-compose.dev.yml CMD [OPTIONS] SERVICE [OPTIONS]
```

### 1. Create an `.env` file

Copy the sample and edit as necessary. Compose automatically sources this environment file for `docker-compose` commands.

```bash
cp .env.sample .env
```

Modify this file with your own settings, but the defaults should be good enough to get going.

### 2. Initialize the database

If running locally, first start the [`server`](#local-postgres-server) service.

Run the following script to configure a Postgres database from scratch:

```bash
bin/initdb.sh
```

Now you can use the [`client`](#pgadmin-client) service to browse the configured Postgres database.

### 3. Build the base image for service jobs

The other services rely on a common `python:3.7`-based image:

```bash
docker-compose build base
```

### 4. Run individual service jobs

Generally, an individual service `SERVICE` can be run with a command like:

```bash
docker-compose run SERVICE [OPTIONS]
```

See the `README` file in each service folder for more details.

### 5. Start a Jupyter Notebook server

`analytics`, `fake` and `ingest` all come with Jupyter Notebook servers that can be run locally:

```bash
bin/notebook.sh SERVICE [ARGS]
```

Now browse to `http://localhost:NB_HOST_PORT` and append the `/?token=<token>` param shown in the Notebook container startup output.

Note your `NB_HOST_PORT` may be different than the default shown in the container output (`8888`).

Also note that all of the services make use of the *same* `NB_HOST_PORT` environment variable, and so they cannot be run at the same time!

Modify `docker-compose.yml` if you need to use different ports to run Notebook servers on multiple services simultaneously.

Optional `[ARGS]` will be passed directly to the `jupyter notebook` startup command. See [bin/notebook.sh](bin/notebook.sh) for details.

## Local Postgres server

Run a local Postgres database server:

```bash
docker-compose up [-d] server
```

The optional `-d` flag runs the container in detached mode, and container output will not be printed to your terminal.

### Configuration

This container uses the following environment variables to create the Postgres server (with defaults shown):

```bash
POSTGRES_HOSTNAME=server
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres_password
```

## pgAdmin client

A web client interface into local and remote Postgres databases:

```bash
docker-compose up [-d] client
```

The optional `-d` flag runs the container in detached mode, and container output will not be printed to your terminal.

Learn more about [pgAdmin][pgadmin-docs] in the documentation.

### Configuration

This container uses the following environment variables to configure pgAdmin (with defaults shown):

```bash
PGADMIN_DEFAULT_EMAIL=user@domain.com
PGADMIN_DEFAULT_PASSWORD=pgadmin_password
PGADMIN_HOST_PORT=8088
```

### Connecting

Once running, connect to the container from a web browser at: `http://localhost:$PGADMIN_HOST_PORT`.

Use the `$PGADMIN_DEFAULT_EMAIL` and `$PGADMIN_DEFAULT_PASSWORD` to log in.

To connect to the Postgres database running in the local [`server`](#local-postgres-server) container,
add a new server connection using the values of the following environment variables (with defaults shown):

```bash
MDS_DB=mds_provider
MDS_USER=mds_provider
MDS_PASSWORD=mds_provider_password
```

[compose]: https://docs.docker.com/compose/overview/
[docker]: https://www.docker.com/
[pgadmin]: https://www.pgadmin.org/
[pgadmin-docs]: https://www.pgadmin.org/docs/pgadmin4/latest/index.html
[postgres]: https://www.postgresql.org/
[provider]: https://github.com/openmobilityfoundation/mobility-data-specification/tree/master/provider
