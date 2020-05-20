# ingest

Pull data from `provider` API endpoints and load to a Postgres database.

## Running

First, copy and edit the sample configuration file:

```bash
cp config.sample.json config.json
```

Ensure the base image is up to date:

```bash
docker-compose build base
```

Run the data ingester:

```bash
docker-compose run [--rm] ingest [OPTIONS]
```

## [OPTIONS]

**Note:** you must provide a range of time to query using some combination of `--start_time`, `--end_time`, and `--duration`.

For a complete list of options, see the help/usage output:

```bash
$ docker-compose run ingest --help

usage: main.py [-h] [--auth_type AUTH_TYPE] [--config CONFIG] [-H HEADERS]
               [--output OUTPUT] [--version VERSION]
               [--columns COLUMNS [COLUMNS ...]] [--device_id DEVICE_ID]
               [--duration DURATION] [--end_time END_TIME] [--events]
               [--no_load] [--no_paging] [--no_validate]
               [--rate_limit RATE_LIMIT] [--registry REGISTRY]
               [--source SOURCE [SOURCE ...]] [--stage_first STAGE_FIRST]
               [--start_time START_TIME] [--status_changes] [--trips]
               [-U [UPDATE_ACTIONS]] [--vehicle_id VEHICLE_ID] [--vehicles]
               provider

Ingest MDS data from various sources.

positional arguments:
  provider              The name or identifier of the provider to query.

optional arguments:
  -h, --help            show this help message and exit
  --auth_type AUTH_TYPE
                        The type used for the Authorization header for
                        requests to the provider (e.g. Basic, Bearer).
  --config CONFIG       Path to a provider configuration file to use.
  -H HEADERS, --header HEADERS
                        One or more 'Header: value' combinations, sent with
                        each request.
  --output OUTPUT       Write results to json files in this directory.
  --version VERSION     The release version at which to reference MDS, e.g.
                        0.3.2
  --columns COLUMNS [COLUMNS ...]
                        One or more column names determining a unique record.
                        Used to drop duplicates in incoming data and detect
                        conflicts with existing records. Columns are reused if
                        multiple record types are requested (e.g.
                        --status_changes and --trips). Make a distinct request
                        per record type to overcome this limitation.
  --device_id DEVICE_ID
                        The device_id to obtain results for. Only valid for
                        --trips and version < 0.4.0.
  --duration DURATION   Number of seconds; with one of --start_time or
                        --end_time, defines a time query range. For version <
                        0.4.0, time query ranges are valid for
                        --status_changes and --trips. For version >= 0.4.0,
                        time query ranges are only valid for --events.
  --end_time END_TIME   The end of the time query range for this request.
                        Should be either numeric Unix time or ISO-8601
                        datetime format. For version < 0.4.0 at least one of
                        end_time or start_time and duration is required. For
                        version >= 0.4.0 end_time is required for all but
                        --vehicles.
  --events              Request events. At least one of --events,
                        --status_changes, --trips, or --vehicles is required.
  --no_load             Do not attempt to load the returned data into a
                        database.
  --no_paging           Return only the first page of data. For version >=
                        0.4.1, has no effect when requesting --status_changes
                        or --trips.
  --no_validate         Do not perform JSON Schema validation against the
                        returned data.
  --rate_limit RATE_LIMIT
                        Number of seconds to pause between paging requests to
                        a given endpoint. For version >= 0.4.1, has no effect
                        when requesting --status_changes or --trips.
  --registry REGISTRY   Path to a providers.csv registry file to use instead
                        of downloading from GitHub.
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
                        ISO-8601 datetime format. For version < 0.4.0, at
                        least one of --end_time or --start_time and --duration
                        is required. For version >= 0.4.0, only valid for
                        --events.
  --status_changes      Request status changes. At least one of --events,
                        --status_changes, --trips, or --vehicles is required.
  --trips               Request trips. At least one of --events,
                        --status_changes, --trips, or --vehicles is required.
  -U [UPDATE_ACTIONS], --on_conflict_update [UPDATE_ACTIONS]
                        Perform an UPDATE when incoming data conflicts with
                        existing database records. Specify one or more
                        'column_name: EXCLUDED.value' to build an ON CONFLICT
                        UPDATE statement. Conflict actions are reused if
                        multiple record types are requested (e.g.
                        --status_changes and --trips). Make a distinct request
                        per record type to overcome this limitation.
  --vehicle_id VEHICLE_ID
                        The vehicle_id to obtain results for. Only valid for
                        --trips and version < 0.4.0.
  --vehicles            Request vehicles. At least one of --events,
                        --status_changes, --trips, or --vehicles is required.
```

## Backfilling

Using all three time parameters together defines a *backfill*, between `start_time` and `end_time`, in blocks of size `duration`.

Step backwards from `end_time` to `start_time`, running the ingestion flow in sliding blocks of size `duration`.

Subsequent blocks overlap the previous block by duration/2 seconds.
Buffers on both ends ensure events starting or ending near a time boundary are captured.

For example:

```bash
--start_time=2018-12-31T00:00:00 --end_time=2019-01-01T00:00:00 --duration=21600
```

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

## Validation

A corollary service to validate a Provider's data feeds and/or local MDS payload files.

After customizing your Provider info in a `config.json` file:

```bash
docker-compose run [--rm] validate [OPTIONS] source [source ...]
```

Where `source` is one or more `provider_name`, `provider_id`, or file paths to MDS payload files. 

### Options

Many of the options work the same as with the `ingest` service:

```bash
$ docker-compose run validate --help

usage: validation.py [-h] [--auth_type AUTH_TYPE] [--config CONFIG]
                     [-H HEADERS] [--output OUTPUT] [--version VERSION]
                     source [source ...]

Validate MDS data feeds.

positional arguments:
  source                The name or identifier of a provider to validate; or
                        One or more paths to (directories containing) MDS
                        Provider JSON file(s) to validate.

optional arguments:
  -h, --help            show this help message and exit
  --auth_type AUTH_TYPE
                        The type used for the Authorization header for
                        requests to the provider (e.g. Basic, Bearer).
  --config CONFIG       Path to a provider configuration file to use.
  -H HEADERS, --header HEADERS
                        One or more 'Header: value' combinations, sent with
                        each request.
  --output OUTPUT       Write results to json files in this directory.
  --version VERSION     The release version at which to reference MDS, e.g.
                        0.3.1
```
