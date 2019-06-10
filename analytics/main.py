"""
Command-line interface implementing various MDS Provider data analytics, including:

  - calculate average availability over a period of time
"""

import argparse
import datetime
import statistics
import time

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
        action="store_true",
        help="Run the availability calculation."
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
        "--version",
        type=lambda v: mds.Version(v),
        default=mds.Version("0.2.1"),
        help="The release version at which to reference MDS, e.g. 0.3.1"
    )

    return parser, parser.parse_args()


def parse_time_range(start=None, end=None, duration=None, version=None):
    """
    Returns a valid range tuple (start, end) given an object with some mix of:
         - start
         - end
         - duration

    If both start and end are present, use those. Otherwise, compute from duration.
    """
    decoder = mds.TimestampDecoder(version=version)

    if start is None and end is None:
        raise ValueError("At least one of start or end is required.")

    if (start is None or end is None) and duration is None:
        raise ValueError("duration is required when only one of start or end is given.")

    if start is not None and end is not None:
        return decoder.decode(start), decoder.decode(end)

    if start is not None:
        start = decoder.decode(start)
        return start, start + datetime.timedelta(seconds=duration)

    if end is not None:
        end = decoder.decode(end)
        return end - datetime.timedelta(seconds=duration), end


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
    debug = kwargs.get("debug")
    step = datetime.timedelta(days=1)

    log(debug, f"Starting calculation for {provider_name}")

    while start < end:
        _end = start + step
        log(debug, f"Counting {start.strftime('%Y-%m-%d')} to {_end.strftime('%Y-%m-%d')}")

        q = query.Availability(
            start,
            _end,
            table="csm_availability_windows",
            provider_name=provider_name,
            vehicle_types=vehicle_type,
            **kwargs
        )

        data = q.get()

        log(debug, f"{len(data)} availability records in time period")

        devices = measure.DeviceCounter(start, _end, **kwargs)

        yield (start, _end, devices.count(data))

        start = _end


if __name__ == "__main__":
    arg_parser, args = setup_cli()

    try:
        start, end = parse_time_range(start=args.start, end=args.end, duration=args.duration, version=args.version)
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
                print(f"{provider_name},{vehicle_type},{_start.strftime('%Y-%m-%d')},{_end.strftime('%Y-%m-%d')},{count.average()},{args.cutoff}")
    else:
        arg_parser.print_help()
        exit(0)
