"""
Command-line interface implementing various MDS Provider data analytics, including:

  - calculate average availability over a period of time
"""

import argparse
from datetime import datetime, timedelta
import dateutil
import mds.providers
from measure import DeviceCounter
import pandas
import query
from statistics import mean
import time


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
        help="A series of PROVIDER=VEHICLE pairs; each pair will be analyzed separately."
    )
    parser.add_argument(
        "--start",
        type=str,
        help="The beginning of the time query range for this request.\
        Should be either int Unix seconds or ISO-8601 datetime format\
        At least one of end or start is required."
    )

    return parser, parser.parse_args()


def parse_time_range(start=None, end=None, duration=None):
    """
    Returns a valid range tuple (start, end) given an object with some mix of:
         - start
         - end
         - duration

    If both start and end are present, use those. Otherwise, compute from duration.
    """
    def _to_datetime(data):
        """
        Helper to parse different textual representations into datetime
        """
        try:
            return datetime.utcfromtimestamp(int(data))
        except:
            return dateutil.parser.parse(data)

    if start is None and end is None:
        raise ValueError("At least one of start or end is required.")

    if (start is None or end is None) and duration is None:
        raise ValueError("duration is required when only one of start or end is given.")

    if start is not None and end is not None:
        return _to_datetime(start), _to_datetime(end)

    duration = int(duration)

    if start is not None:
        start = _to_datetime(start)
        return start, start + timedelta(seconds=duration)

    if end is not None:
        end = _to_datetime(end)
        return end - timedelta(seconds=duration), end


def log(debug, msg):
    """
    Prints the message if debugging is turned on.
    """
    def __now():
        return datetime.utcnow().isoformat()

    if debug:
        print(f"[{__now()}] {msg}")


def availability(provider_name, vehicle_type, start, end, **kwargs):
    """
    Runs the availability calculation
    """
    debug = kwargs.get("debug")
    step = timedelta(days=1)

    results = {}

    log(debug, f"Starting calculation for {provider_name}")

    while start < end:
        _end = start + step
        log(debug, f"Counting {start.strftime('%Y-%m-%d')} to {_end.strftime('%Y-%m-%d')}")

        q = query.Availability(
            start,
            _end,
            table="csm_availability_windows",
            vehicle_types=vehicle_type,
            **kwargs
        )

        data = q.get(provider_name=provider_name)

        log(debug, f"{len(data)} availability records in time period")

        devices = DeviceCounter(start, _end, **kwargs)

        yield (start, _end, devices.count(data))

        start = _end


if __name__ == "__main__":
    arg_parser, args = setup_cli()

    try:
        start, end = parse_time_range(start=args.start, end=args.end, duration=args.duration)
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
                print(f"{provider_name},{vehicle_type},{start.strftime('%Y-%m-%d')},{end.strftime('%Y-%m-%d')},{count.average()}")
    else:
        arg_parser.print_help()
        exit(0)
