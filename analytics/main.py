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
        "--output",
        action="store_true",
        help="Write results to csv."
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


def parse_time_range(args):
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

    if args.start is not None and args.end is not None:
        return _to_datetime(args.start), _to_datetime(args.end)

    duration = int(args.duration)

    if args.start is not None:
        start = _to_datetime(args.start)
        return start, start + timedelta(seconds=duration)

    if args.end is not None:
        end = _to_datetime(args.end)
        return end - timedelta(seconds=duration), end


def log(args, msg):
    """
    Prints the message if debugging is turned on.
    """
    def __now():
        return datetime.utcnow().isoformat()

    if args.debug:
        print(f"[{__now()}] {msg}")


def availability(provider_name, vehicle_type, args):
    """
    Runs the availability calculation
    """
    oneday = timedelta(days=1)

    log(args, f"""Starting availability calculation:
    - time range: {args.start} to {args.end}
    - provider: {provider_name}
    - vehicle type: {vehicle_type}""")

    devices = DeviceCounter(args.start, args.end, local=args.local, debug=args.debug)

    q = query.Availability(args.start, args.end,
        vehicle_types=vehicle_type,
        table="csm_availability_windows",
        local=args.local, debug=True)

    results = {}

    log(args, f"Starting calculation for {provider_name}")

    data = q.get(provider_name=provider_name)
    partition = devices.count(data).partition()

    log(args, partition.describe())

    overall_avg = devices.average()
    log(args, f"Overall average: {overall_avg}")

    counts = {}
    start = args.start

    while start < args.end:
        end = start + oneday
        log(args, f"Counting {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}")

        _q = query.Availability(start, end,
            vehicle_types=vehicle_type,
            table="csm_availability_windows",
            local=args.local, debug=args.debug)

        _data = _q.get(provider_name=provider_name)

        log(args, f"{len(_data)} availability records in time period")

        _devices = DeviceCounter(start, end, local=args.local, debug=args.debug)
        counts[start] = _devices.count(_data)

        start = end

    if args.debug:
        for date, count in counts.items():
            print(f"{provider_name},{vehicle_type},{date.strftime('%Y-%m-%d')},{count.average()},{overall_avg}")

    return overall_avg, counts


if __name__ == "__main__":
    arg_parser, args = setup_cli()

    # assert the time range parameters and parse a valid range
    if args.start is None and args.end is None:
        arg_parser.print_help()
        exit(1)

    if (args.start is None or args.end is None) and args.duration is None:
        arg_parser.print_help()
        exit(1)

    args.start, args.end = parse_time_range(args)

    if args.availability:
        for provider_name, vehicle_type in dict(args.queries).items():
            overall_avg, counts = availability(provider_name, vehicle_type, args)
            if args.output:
                for date, count in counts.items():
                    print(f"{provider_name},{vehicle_type},{date.strftime('%Y-%m-%d')},{count.average()},{overall_avg}")
    else:
        arg_parser.print_help()
        exit(0)
