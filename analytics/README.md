# analytics

Perform analysis on an MDS `provider` Postgres database.

## Running

First, ensure the image is up to date locally:

```bash
docker-compose build --no-cache analytics
```

Then run an analytics job:

```bash
docker-compose run --rm analytics [OPTIONS]
```

## [OPTIONS]

Note: you must provide a range of time to query using some combination of `start`, `end`, and `duration`. Providing both `start` and `end` takes precedence over either of them with `duration`.

### `--availability`

Run the availability calculation.

### `--debug`

Print debug messages.

### `--duration DURATION`

Number of seconds; with `--start` or `--end` defines a time query range.

### `--end END_TIME`

The end of the time query range for this job. Should be either int Unix seconds or ISO-8061 datetime format.

### `--local`

Input and query times are local.

### `--query PROVIDER=VEHICLE`

A `provider_name` and `vehicle_type` pair to analyze. Use this option multiple times to analyze multiple pairs independently.

E.g.

```bash
--query providerA=scooter --query providerA=bicycle --query providerB=scooter
```

### `--start START_TIME`

The beginning of the time query range for this job. Should be either int Unix seconds or ISO-8061 datetime format.
