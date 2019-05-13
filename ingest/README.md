# ingest

Pull data from `provider` API endpoints and load to a Postgres database.

## Running

First, copy and edit the sample configuration file:

```bash
cp config.sample.json config.json
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

**Note:** you must provide a range of time to query using some combination of `--start_time`, `--end_time`, and `--duration`.

For a complete list of options, see the help/usage output:

```bash
$ docker-compose run ingest --help

usage: main.py [-h] [--auth_type AUTH_TYPE] [--columns COLUMNS [COLUMNS ...]]
               [--config CONFIG] [--device_id DEVICE_ID] [--duration DURATION]
               [--end_time END_TIME] [-H HEADERS] [--no_load] [--no_paging]
               [--no_validate] [--output OUTPUT] [--rate_limit RATE_LIMIT]
               [--registry REGISTRY] [--source SOURCE [SOURCE ...]]
               [--stage_first STAGE_FIRST] [--start_time START_TIME]
               [--status_changes] [--trips] [-U [UPDATE_ACTIONS]]
               [--vehicle_id VEHICLE_ID] [--version VERSION]
               provider

Ingest MDS data from various sources.

positional arguments:
  provider              The name or identifier of the provider to query.

optional arguments:
  -h, --help            show this help message and exit
  --auth_type AUTH_TYPE
                        The type used for the Authorization header for
                        requests to the provider (e.g. Basic, Bearer).
  --columns COLUMNS [COLUMNS ...]
                        One or more column names determining a unique record.
                        Used to drop duplicates in incoming data and detect
                        conflicts with existing records. NOTE: the program
                        does not differentiate between --columns for
                        --status_changes or --trips.
  --config CONFIG       Path to a provider configuration file to use.
  --device_id DEVICE_ID
                        The device_id to obtain results for. Only applies to
                        --trips.
  --duration DURATION   Number of seconds; with one of --start_time or
                        --end_time, defines a time query range. With both,
                        defines a backfill window size.
  --end_time END_TIME   The end of the time query range for this request.
                        Should be either int Unix seconds or ISO-8601 datetime
                        format. At least one of end_time or start_time is
                        required.
  -H HEADERS, --header HEADERS
                        One or more 'Header: value' combinations, sent with
                        each request.
  --no_load             Do not attempt to load the returned data.
  --no_paging           Return only the first page of data.
  --no_validate         Do not perform JSON Schema validation against the
                        returned data.
  --output OUTPUT       Write results to json files in this directory.
  --rate_limit RATE_LIMIT
                        Number of seconds to pause between paging requests to
                        a given endpoint.
  --registry REGISTRY   Local file path to a providers.csv registry file to
                        use instead of downloading from GitHub.
  --source SOURCE [SOURCE ...]
                        One or more paths to (directories containing) MDS
                        Provider JSON file(s)
  --stage_first STAGE_FIRST
                        False to append records directly to the data table.
                        True to stage in a temp table before UPSERT to the
                        data table. Int to increase randomness of the temp
                        table name.
  --start_time START_TIME
                        The beginning of the time query range for this
                        request. Should be either numeric Unix time or
                        ISO-8601 datetime format. At least one of end_time or
                        start_time is required.
  --status_changes      Request status changes. At least one of
                        --status_changes or --trips is required.
  --trips               Request trips. At least one of --status_changes or
                        --trips is required.
  -U [UPDATE_ACTIONS], --on_conflict_update [UPDATE_ACTIONS]
                        Perform an UPDATE when incoming data conflicts with
                        existing database records. Specify one or more
                        'column_name: EXCLUDED.value' to build an ON CONFLICT
                        UPDATE statement. NOTE: the program does not
                        differentiate between --on_conflict_update for
                        --status_changes or --trips.
  --vehicle_id VEHICLE_ID
                        The vehicle_id to obtain results for. Only applies to
                        --trips.
  --version VERSION     The release version at which to reference MDS, e.g.
                        0.3.1
```

## Backfilling

Using all three time parameters together defines a *backfill*, between `start_time` and `end_time`, in blocks of size `duration`.

Step backwards from `end_time` to `start_time`, running the ingestion flow in sliding blocks of size `duration`.

Subsequent blocks overlap the previous block by duration/2 seconds.
Buffers on both ends ensure events starting or ending near a time boundary are captured.

For example:

    --start_time=2018-12-31T00:00:00 --end_time=2019-01-01T00:00:00 --duration=21600

Results in the following backfill requests:

* `2018-12-31T21:00:00` to `2019-01-01T03:00:00`
* `2018-12-31T18:00:00` to `2018-12-31T00:00:00`
* `2018-12-31T15:00:00` to `2018-12-31T21:00:00`
* `2018-12-31T12:00:00` to `2018-12-31T18:00:00`
* `2018-12-31T09:00:00` to `2018-12-31T15:00:00`
* `2018-12-31T06:00:00` to `2018-12-31T12:00:00`
* `2018-12-31T03:00:00` to `2018-12-31T09:00:00`
* `2018-12-31T00:00:00` to `2018-12-31T06:00:00`
* `2018-12-30T21:00:00` to `2018-12-31T03:00:00`

Backfills ignore the `--no_paging` flag, always requesting all pages.
