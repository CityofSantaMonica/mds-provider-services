"""
Command-line interface implementing various MDS Provider data analytics, including:

  - calculate average availability over a period of time
"""

import argparse
import datetime

import mds

import measure
import query


def setup_cli():
    """
    Create the cli argument interface, and parses incoming args.

    Returns a tuple:
        - the argument parser
        - the parsed args
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--availability",
        const="csm_availability_windows",
        default=False,
        nargs="?",
        help="Run the availability calculation.\
        Optionally provide the view/table to query for availability windows."
    )
    parser.add_argument(
        "--cutoff",
        type=int,
        default=-1,
        help="Maximum allowed length of a time-windowed event (e.g. availability window, trip), in days."
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print debug messages."
    )
    parser.add_argument(
        "--duration",
        type=int,
        help="Number of seconds; with --start_time or --end_time, defines a time query range."
    )
    parser.add_argument(
        "--end",
        type=str,
        help="The end of the time query range for this request.\
        Should be either int Unix seconds or ISO-8601 datetime format.\
        At least one of end or start is required."
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Input and query times are local."
    )
    parser.add_argument(
        "--query",
        action="append",
        type=lambda kv: kv.split("=", 1),
        dest="queries",
        metavar="QUERY",
        help="A {provider_name}={vehicle_type} pair; multiple pairs will be analyzed separately."
    )
    parser.add_argument(
        "--start",
        type=str,
        help="The beginning of the time query range for this request.\
        Should be either int Unix seconds or ISO-8601 datetime format\
        At least one of end or start is required."
    )
    parser.add_argument(
        "--save",
        const=True,
        default=False,
        nargs="?",
        help="Save calculation results to the database.\
        Optionally provide the destination table to write results."
    )
    parser.add_argument(
        "--version",
        type=lambda v: mds.Version(v),
        default=mds.Version("0.3.2"),
        help="The release version at which to reference MDS, e.g. 0.3.1"
    )

    return parser, parser.parse_args()


def parse_time_range(**kwargs):
    """
    Returns a valid range tuple (start_time, end_time) given some mix of:

    * start_time: datetime
    * end_time: datetime
    * duration: numeric seconds

    If both start_time and end_time are given, use those. Otherwise, compute from duration.
    """
    decoder = mds.encoding.TimestampDecoder(version=kwargs["version"])

    if "start_time" in kwargs and "end_time" in kwargs and kwargs["start_time"] and kwargs["end_time"]:
        try:
            start_time, end_time = decoder.decode(int(kwargs["start_time"])), decoder.decode(int(kwargs["end_time"]))
        except ValueError:
            start_time, end_time = decoder.decode(kwargs["start_time"]), decoder.decode(kwargs["end_time"])
        return (start_time, end_time) if start_time <= end_time else (end_time, start_time)

    duration = datetime.timedelta(seconds=kwargs["duration"])

    if "start_time" in kwargs and kwargs["start_time"]:
        try:
            start_time = decoder.decode(int(kwargs["start_time"]))
        except ValueError:
            start_time = decoder.decode(kwargs["start_time"])
        return start_time, start_time + duration

    if "end_time" in kwargs and kwargs["end_time"]:
        try:
            end_time = decoder.decode(int(kwargs["end_time"]))
        except ValueError:
            end_time = decoder.decode(kwargs["end_time"])
        return end_time - duration, end_time


def log(debug, msg):
    """
    Prints the message if debugging is turned on.
    """
    def _now():
        return datetime.datetime.utcnow().isoformat()

    if debug:
        print(f"[{_now()}] {msg}")


def availability(provider_name, vehicle_type, start, end, **kwargs):
    """
    Runs the availability calculation
    """
    source = kwargs.get("availability")
    debug = kwargs.get("debug")
    step = datetime.timedelta(days=1)

    log(debug, f"Starting calculation for {provider_name}")
    log(debug, f"Reading windows from {source}")

    while start < end:
        _end = start + step
        log(debug, f"Counting {start.strftime('%Y-%m-%d')} to {_end.strftime('%Y-%m-%d')}")

        q = query.Availability(
            start,
            _end,
            source=source,
            provider_name=provider_name,
            vehicle_types=vehicle_type,
            **kwargs
        )

        data = q.get()

        log(debug, f"{len(data)} availability records in time period")

        devices = measure.DeviceCounter(start, _end, **kwargs)

        yield (start, _end, devices.count(data))

        start = _end


def save_availability_count(dest, *args):
    """
    Insert an availability calculation result into the database.
    """
    dest = dest if isinstance(dest, str) else "availability_counts"
    columns = ["provider_name", "vehicle_type", "start_time", "end_time", "avg_availability", "cutoff"]

    # insert columns and values
    inserts = ", ".join(columns)
    values = ", ".join(map(lambda x: "%s", columns))

    # SQL to insert and overwrite on conflict
    sql = f"""
    INSERT INTO "{dest}"
    ({inserts})
    VALUES
    ({values})
    ON CONFLICT ON CONSTRAINT unique_count DO UPDATE SET avg_availability = EXCLUDED.avg_availability
    ;
    """

    with query.ENGINE.begin() as conn:
        conn.execute(sql, args)


if __name__ == "__main__":
    arg_parser, args = setup_cli()

    try:
        start, end = parse_time_range(start_time=args.start, end_time=args.end, duration=args.duration, version=args.version)
    except ValueError as e:
        print(e)
        arg_parser.print_help()
        exit(1)

    queries = dict(args.queries)

    kwargs = vars(args)
    for key in ("start", "end", "duration", "queries"):
        del kwargs[key]

    if args.availability:
        for provider_name, vehicle_type in queries.items():
            for _start, _end, count in availability(provider_name, vehicle_type, start, end, **kwargs):
                avg = count.average()

                if args.save:
                    save_availability_count(args.save, provider_name, vehicle_type, _start, _end, avg, args.cutoff)

                print(f"{provider_name},{vehicle_type},{_start.strftime('%Y-%m-%d')},{_end.strftime('%Y-%m-%d')},{avg},{args.cutoff}")
    else:
        arg_parser.print_help()
        exit(0)
