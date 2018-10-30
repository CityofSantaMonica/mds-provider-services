# mds-provider-services

Services for working with [MDS `provider`][provider] data, built as runnable Docker containers.

These services are implemented via [`mds-provider`](https://github.com/CityofSantaMonica/mds-provider),
a general-purpose Python library for working with MDS Provider data.

## Batteries Included

The services are organized around specific functions. More detailed explanation can be found in service `README` files.

| service | description |
| --------- | ----------- |
| [`client`](client/) | [pgAdmin4][pgadmin] web client |
| [`db`](db/) | Work with a `provider` database |
| [`fake`](fake/) | Generate fake `provider` data for testing and development |
| [`ingest`](ingest/) | Ingest `provider` data from different sources |
| [`server`](server/) | Local [postgres][postgres] database server |

## Getting Started

Requires both [Docker][docker] and [Docker Compose][compose].

Commands below should be run from the root of this repository, where the `docker-compose.yml` file lives.

### 0. Create a `docker-compose.yml` file

Copy the `dev` file and edit as necessary. Compose automatically uses this file for service definitions and configuration.

You shouldn't have to make too many (if any) changes; see the next step for environment variable configuration.

```console
cp docker-compose.dev.yml docker-compose.yml
```

Alternatively, use the `dev` file as-is by prepending a switch to `docker-compose` commands, e.g.:

```console
docker-compose -f docker-compose.dev.yml <cmd> <cmd options> <service> <service options>
```

### 1. Create an `.env` file

Copy the sample and edit as necessary. Compose automatically sources this environment file for `docker-compose` commands.

```console
cp .env.sample .env
```

You will want to modify this file with your own settings; but the defaults should be good enough for the next step.

### 2. Initialize the database

Build and start the necessary containers to load and explore a Postgres database.

```console
bin/initdb.sh
```

Now you can browse to `http://localhost:PGADMIN_HOST_PORT` and login with the `PGADMIN_DEFAULT` credentials.

Attach to the server `POSTGRES_HOSTNAME`, database `MDS_DB`, with the `MDS` credentials.

### 3. Run individual service jobs

Generate some [`fake`](fake/) data or [`ingest`](ingest/) and validate data feeds.

See the `README` file in each service folder for more details.

### 4. Start a Jupyter Notebook server

`fake` and `ingest` both come with Jupyter Notebook servers that can be run for development and testing purposes:

```console
bin/notebook.sh <service-name>
```

Now browse to `http://localhost:NB_HOST_PORT` and append the `/?token=<token>` param shown in the notebook container startup output.

Note your `NB_HOST_PORT` may be different than the default shown in the container output (`8888`).

Also note that both `fake` and `ingest` make use of the *same* `NB_HOST_PORT` environment variable, and so both cannot be run at the same time!

[compose]: https://docs.docker.com/compose/overview/
[docker]: https://www.docker.com/
[pgadmin]: https://www.pgadmin.org/
[postgres]: https://www.postgresql.org/
[provider]: https://github.com/CityOfLosAngeles/mobility-data-specification/tree/master/provider
