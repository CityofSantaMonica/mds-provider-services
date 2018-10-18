# mds-provider-services

Services for working with [MDS `provider`][provider] data, built as runnable Docker containers.

These services are implemented via [`mds-provider`](https://github.com/CityofSantaMonica/mds-provider),
a general-purpose Python library for working with MDS Provider data.

## Running the containers

Requires both [Docker][docker] and [Docker Compose][compose].

`docker-compose` commands below should be run from the root of this repository,
where the `docker-compose.yml` file lives.

### Container organization

The containers are organized around specific services and functions. More detailed explanation can be found in a container's `README` file.

| container | description |
| --------- | ----------- |
| [`client`](client/) | [pgAdmin4][pgadmin] web client |
| [`fake`](fake/) | Generate fake `provider` data for testing and development |
| [`initdb`](initdb/) | Initialize a `provider` database |
| [`ingest`](ingest/) | Ingest `provider` data from different sources |
| [`server`](server/) | Local [postgres][postgres] database server |

## Getting Started

### 0. Create a `docker-compose.yml` file

Copy the `dev` file and edit as necessary. Compose automatically uses this file for service definitions and configuration.

You *shouldn't* have to make too many (if any) changes; see the next step for environment variable configuration.

```console
$ cp docker-compose.dev.yml docker-compose.yml
```

Alternatively, use the `dev` file as-is by prepending a switch to `docker-compose` commands, e.g.:

```console
$ docker-compose -f docker-compose.dev.yml <cmd> <cmd options> <service> <service options>
```

### 1. Create an `.env` file

Copy the sample and edit as necessary. Compose automatically sources this
environment file for `docker-compose` commands.

```console
$ cp .env.sample .env
```

### 2. Initialize the database

Build and start the necessary containers according to the dependencies outlined in
[`docker-compose.yml`](docker-compose.yml).

```bash
$ docker-compose up -d --build --force-recreate initdb
```

### 3. Run individual container jobs

See the `README` file in each container folder for more details.

[compose]: https://docs.docker.com/compose/overview/
[docker]: https://www.docker.com/
[pgadmin]: https://www.pgadmin.org/
[postgres]: https://www.postgresql.org/
[provider]: https://github.com/CityOfLosAngeles/mobility-data-specification/tree/master/provider
