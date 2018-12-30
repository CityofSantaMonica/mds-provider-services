# ingest

Pull data from `provider` API endpoints and load to a Postgres database.

## Running

First, copy and edit the sample configuration file:

```bash
cp .config.sample .config
```

Next, ensure the image is up to date locally:

```bash
docker-compose build --no-cache ingest
```

Then run the data ingester:

```bash
docker-compose run --rm ingest [OPTIONS]
```

## [OPTIONS]

Note: you must provide a range of time to query using some combination of `start_time`, `end_time`, and `duration`.

### Backfilling

Using all three time parameters together defines a *backfill*, between `start_time` and `end_time`, in blocks of size `duration`.

Subsequent backfill blocks overlap the previous block by `duration/2` seconds. Buffers on both ends ensure events
starting or ending near a time boundary are captured.

For example `2018-12-31` with a 6 hour duration:

```bash
--start_time 2018-12-31T00:00:00 --end_time 2018-12-31T23:59:59 --duration 21600
```

Results in the following backfill requests:

- `2018-12-31 20:59:59` to `2019-01-01 02:59:59`
- `2018-12-31 17:59:59` to `2018-12-31 23:59:59`
- `2018-12-31 14:59:59` to `2018-12-31 20:59:59`
- `2018-12-31 11:59:59` to `2018-12-31 17:59:59`
- `2018-12-31 08:59:59` to `2018-12-31 14:59:59`
- `2018-12-31 05:59:59` to `2018-12-31 11:59:59`
- `2018-12-31 02:59:59` to `2018-12-31 08:59:59`
- `2018-12-30 23:59:59` to `2018-12-31 05:59:59`
- `2018-12-30 20:59:59` to `2018-12-31 02:59:59`

Backfills ignore the `no_paging` flag, always requesting all pages.

### `--bbox BBOX`

The bounding-box with which to restrict the results of this request.

The order is (separated by commas):

- southwest longitude,
- southwest latitude,
- northeast longitude,
- northeast latitude

For example:

```bash
--bbox -122.4183,37.7758,-122.4120,37.7858
```

### `--config CONFIG`

Path to a provider configuration file to use. The default is `.config`.

### `--device_id DEVICE_ID`

The device_id to obtain results for. Only applies to `--trips`.

### `--duration DURATION`

Number of seconds; with `--start_time` or `--end_time` defines a time query range.

### `--end_time END_TIME`

The end of the time query range for this request. Should be either int Unix seconds or ISO-8601 datetime format.

### `--no_load`

Do not attempt to load any returned data into a database.

### `--no_paging`

Flag indicating paging through the response should not occur. Return only the first page of data.

### `--no_validate`

Do not perform JSON Schema validation against the returned data.

### `--on_conflict_update`

Instead of ignoring, perform an UPDATE when incoming data conflicts with existing rows in the database.

### `--output OUTPUT`

Write data into JSON files in this path.

### `--providers PROVIDER [PROVIDER ...]`

One or more `provider_name` to query. The default is to query all configured providers.

### `--ref REF`

Git branch name, commit hash, or tag at which to reference MDS. The default is `master`.

### `--source SOURCE [SOURCE ...]`

One or more paths to (directories containing) MDS Provider JSON file(s). These will be read instead of requesting from Provider APIs.

The `--status_changes` and `--trips` flags will be respected if possible from the source file names.

Note that `--bbox`, `--end_time`, and other related querystring paramters don't apply for `--source`.

### `--stage_first STAGE_FIRST`

`False` to append records directly to the target tables. `True` to stage in a temp table before UPSERT.

Given an `int` greater than 0, determines the degrees of randomness when creating the temp table, e.g.

```bash
--stage_first 3
```

stages to a random temp table with `26*26*26` possible naming choices.

### `--start_time START_TIME`

The beginning of the time query range for this request. Should be either int Unix seconds or ISO-8601 datetime format.

### `--status_changes`

Flag indicating Status Changes should be requested.

### `--trips`

Flag indicating Trips should be requested.

### `--vehicle_id VEHICLE_ID`

The `vehicle_id` to obtain results for. Only applies to `--trips`.
