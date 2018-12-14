# ingest

Pull data from `provider` API endpoints and load to a Postgres database.

## Running

First, copy and edit the sample configuration file:

```console
cp .config.sample .config
```

Next, ensure the image is up to date locally:

```console
docker-compose build --no-cache ingest
```

Then run the data ingester:

```console
docker-compose run --rm ingest [OPTIONS]
```

## [OPTIONS]

Note: you must provide a range of time to query using some combination of `start_time`, `end_time`, and `duration`. Providing both `start_time` and `end_time` takes precedence over either of them with `duration`.

### `--bbox BBOX`

The bounding-box with which to restrict the results of this request.

The order is (separated by commas):

* southwest longitude,
* southwest latitude,
* northeast longitude,
* northeast latitude

For example:

```console
--bbox -122.4183,37.7758,-122.4120,37.7858
```

### `--config CONFIG`

Path to a provider configuration file to use. The default is `.config`.

### `--device_id DEVICE_ID`

The device_id to obtain results for. Only applies to `--trips`.

### `--duration DURATION`

Number of seconds; with `--start_time` or `--end_time` defines a time query range.

### `--end_time END_TIME`

The end of the time query range for this request. Should be either int Unix seconds or ISO-8061 datetime format.

### `--no_load`

Do not attempt to load any returned data into a database.

### `--no_paging`

Flag indicating paging through the response should *not* occur. Return only the first page of data.

### `--no_validate`

Do not perform JSON Schema validation against the returned data.

### `--output OUTPUT`

Write data into JSON files in this path.

### `--providers PROVIDER [PROVIDER ...]`

One or more providers to query, identified either by `provider_name` or `provider_id`.

The default is to query all configured providers.

### `--ref REF`

Git branch name, commit hash, or tag at which to reference MDS. The default is `master`.

### `--source SOURCE [SOURCE ...]`

One or more paths to (directories containing) MDS Provider JSON file(s). These will be read instead of requesting from Provider APIs.

The `--status_changes` and `--trips` flags will be respected if possible from the source file names.

Note that `--bbox`, `--end_time`, and other related querystring paramters don't apply for `--source`.

### `--start_time START_TIME`

The beginning of the time query range for this request. Should be either int Unix seconds or ISO-8061 datetime format.

### `--status_changes`

Flag indicating Status Changes should be requested.

### `--trips`

Flag indicating Trips should be requested.

### `--vehicle_id VEHICLE_ID`

The `vehicle_id` to obtain results for. Only applies to `--trips`.
