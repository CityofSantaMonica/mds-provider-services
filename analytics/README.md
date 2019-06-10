# analytics

Perform analysis on an MDS `provider` Postgres database.

## Running

First, ensure the image is up to date locally:

```bash
docker-compose build --no-cache analytics
```

Then run an analytics job:

```bash
docker-compose run [--rm] analytics [OPTIONS]
```

## [OPTIONS]

**Note:** you must provide a range of time to query using some combination of `--start_time`, `--end_time`, and `--duration`.

For a complete list of options, see the help/usage output:

```bash
$ docker-compose run analytics --help

usage: main.py [-h] [--availability] [--cutoff CUTOFF] [--debug]
               [--duration DURATION] [--end END] [--local] [--query QUERIES]
               [--start START]

optional arguments:
  -h, --help           show this help message and exit
  --availability       Run the availability calculation.
  --cutoff CUTOFF      Maximum allowed length of a time-windowed event (e.g.
                       availability window, trip), in days.
  --debug              Print debug messages.
  --duration DURATION  Number of seconds; with --start_time or --end_time,
                       defines a time query range.
  --end END            The end of the time query range for this request.
                       Should be either int Unix seconds or ISO-8601 datetime
                       format. At least one of end or start is required.
  --local              Input and query times are local.
  --query QUERIES      A series of PROVIDER=VEHICLE pairs; each pair will be
                       analyzed separately.
  --start START        The beginning of the time query range for this request.
                       Should be either int Unix seconds or ISO-8601 datetime
                       format At least one of end or start is required.
```
