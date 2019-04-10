"""
Command-line interface implementing various MDS Provider data ingestion flows, including:

  - request data from one or more Provider endpoints
  - validate data against any version of the schema
  - save data to the filesystem and/or load it into Postgres

All fully customizable through extensive parameterization and configuration options.
"""

from acquire import acquire_data, provider_names
import argparse
from configparser import ConfigParser
from datetime import datetime, timedelta
import dateutil.parser
import json
from load import load_data
import mds
from mds.api import ProviderClient
import mds.providers
import os
from pathlib import Path
import sys
import time
from validate import validate_data


def setup_cli():
    """
    Create the cli argument interface, and parses incoming args.

    Returns a tuple:
        - the argument parser
        - the parsed args
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--bbox",
        type=str,
        help="The bounding-box with which to restrict the results of this request.\
        The order is southwest longitude, southwest latitude, northeast longitude, northeast latitude.\
        For example: --bbox -122.4183,37.7758,-122.4120,37.7858"
    )
    parser.add_argument(
        "--config",
        type=str,
        default=os.path.join(os.getcwd(), ".config"),
        help="Path to a provider configuration file to use."
    )
    parser.add_argument(
        "--device_id",
        type=str,
        help="The device_id to obtain results for. Only applies to --trips."
    )
    parser.add_argument(
        "--duration",
        type=int,
        help="Number of seconds; with one of --start_time or --end_time, defines a time query range.\
        With both, defines a backfill window size."
    )
    parser.add_argument(
        "--end_time",
        type=str,
        help="The end of the time query range for this request.\
        Should be either int Unix seconds or ISO-8601 datetime format.\
        At least one of end_time or start_time is required."
    )
    parser.add_argument(
        "--no_load",
        action="store_true",
        help="Do not attempt to load the returned data."
    )
    parser.add_argument(
        "--no_paging",
        action="store_true",
        help="Flag indicating paging through the response should *not* occur. Return only the first page of data."
    )
    parser.add_argument(
        "--no_validate",
        action="store_true",
        help="Do not perform JSON Schema validation against the returned data."
    )
    parser.add_argument(
        "--on_conflict_update",
        action="store_true",
        help="Instead of ignoring, perform an UPDATE when incoming data conflicts with existing rows in the database."
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Write results to json files in this directory."
    )
    parser.add_argument(
        "--providers",
        type=str,
        nargs="+",
        help="One or more provider names to query, separated by commas.\
        The default is to query all configured providers."
    )
    parser.add_argument(
        "--rate_limit",
        type=int,
        default=0,
        help="Number of seconds to pause between paging requests to a given endpoint."
    )
    parser.add_argument(
        "--ref",
        type=str,
        help="Git branch name, commit hash, or tag at which to reference MDS."
    )
    parser.add_argument(
        "--registry",
        type=str,
        help="Local file path to a providers.csv registry file to use instead of downloading from GitHub."
    )
    parser.add_argument(
        "--source",
        type=str,
        nargs="+",
        help="One or more paths to (directories containing) MDS Provider JSON file(s)"
    )
    parser.add_argument(
        "--stage_first",
        default=True,
        help="False to append records directly to the data tables. True to stage in a temp table before UPSERT,\
        or an int to stage in a temp table before UPSERT, with increasing randomness to the temp table name."
    )
    parser.add_argument(
        "--start_time",
        type=str,
        help="The beginning of the time query range for this request.\
        Should be either int Unix seconds or ISO-8601 datetime format.\
        At least one of end_time or start_time is required."
    )
    parser.add_argument(
        "--status_changes",
        action="store_true",
        help="Flag indicating Status Changes should be requested."
    )
    parser.add_argument(
        "--trips",
        action="store_true",
        help="Flag indicating Trips should be requested."
    )
    parser.add_argument(
        "--vehicle_id",
        type=str,
        help="The vehicle_id to obtain results for. Only applies to --trips."
    )

    return parser, parser.parse_args()


def parse_config(path):
    """
    Helper to parse a config file at :path:.
    """
    if not os.path.exists(path):
        print("Could not find config file: ", path)
        exit(1)

    print("Reading config file:", path)

    config = ConfigParser()
    config.read(path)

    return config


def parse_time_range(args):
    """
    Returns a valid range tuple (start_time, end_time) given an object with some mix of:
         - start_time
         - end_time
         - duration

    If both start_time and end_time are present, use those. Otherwise, compute from duration.
    """
    def _to_datetime(data):
        """
        Helper to parse different textual representations into datetime
        """
        try:
            return datetime.utcfromtimestamp(int(data))
        except:
            return dateutil.parser.parse(data)

    if args.start_time is not None and args.end_time is not None:
        start_time, end_time = _to_datetime(args.start_time), _to_datetime(args.end_time)
        return (start_time, end_time) if start_time <= end_time else (end_time, start_time)

    if args.start_time is not None:
        start_time = _to_datetime(args.start_time)
        return start_time, start_time + timedelta(seconds=args.duration)

    if args.end_time is not None:
        end_time = _to_datetime(args.end_time)
        return end_time - timedelta(seconds=args.duration), end_time


def output_data(output, payloads, record_type, start_time, end_time):
    """
    Write data to json files in the a directory.
    """
    def _file_name(output, provider):
        """
        Generate a filename from the given parameters.
        """
        fname = f"{provider}_{record_type}_{start_time.isoformat()}_{end_time.isoformat()}.json"
        return os.path.join(output, fname)

    for provider, payload in payloads.items():
        fname = _file_name(output, provider.provider_name)
        with open(fname, "w") as f:
            json.dump(payload, f)


def backfill(record_type, client, start_time, end_time, duration, **kwargs):
    """
    Step backwards from :end_time: to :start_time:, running the ingestion flow in sliding blocks of size :duration:.

    Subsequent blocks overlap the previous block by :duration:/2 seconds. Buffers on both ends ensure events starting or
    ending near a time boundary are captured.

    For example:

    :start_time:=datetime(2018,12,31,0,0,0), :end_time:=datetime(2018,12,31,23,59,59), :duration:=21600

    Results in the following backfill requests:

    - 2018-12-31T20:59:59 to 2019-01-01T02:59:59
    - 2018-12-31T17:59:59 to 2018-12-31T23:59:59
    - 2018-12-31T14:59:59 to 2018-12-31T20:59:59
    - 2018-12-31T11:59:59 to 2018-12-31T17:59:59
    - 2018-12-31T08:59:59 to 2018-12-31T14:59:59
    - 2018-12-31T05:59:59 to 2018-12-31T11:59:59
    - 2018-12-31T02:59:59 to 2018-12-31T08:59:59
    - 2018-12-30T23:59:59 to 2018-12-31T05:59:59
    - 2018-12-30T20:59:59 to 2018-12-31T02:59:59
    """
    kwargs["no_paging"] = False
    kwargs["client"] = client
    rate_limit = kwargs.get("rate_limit")

    duration = timedelta(seconds=duration)
    offset = duration / 2
    end = end_time + offset

    while end >= start_time:
        start = end - duration
        ingest(record_type, **kwargs, start_time=start, end_time=end)
        end = end - offset

        if rate_limit:
            time.sleep(rate_limit)


def ingest(record_type, **kwargs):
    """
    Run the ingestion flow for the given :record_type:.
    """
    datasource = acquire_data(record_type, **kwargs)

    validating = not kwargs.get("no_validate")
    if validating:
        ref = kwargs.get("ref")
        datasource = validate_data(datasource, record_type, ref)

        # clean up missing data
        for k in [k for k in datasource.keys()]:
            if not datasource[k] or len(datasource[k]) < 1:
                del datasource[k]
    else:
        print("Skipping data validation")

    # output to files if needed
    output = kwargs.get("output")
    if output and os.path.exists(output):
        print(f"Writing data files to {output}")
        start_time, end_time = kwargs.get("start_time"), kwargs.get("end_time")
        output_data(output, datasource, record_type, start_time, end_time)

    loading = not kwargs.get("no_load")
    if loading and len(datasource) > 0:
        load_data(datasource, record_type, **kwargs)

    print(f"{record_type} complete")


if __name__ == "__main__":
    now = datetime.utcnow().isoformat()
    print(f"Run time: {now}")

    # configuration
    arg_parser, args = setup_cli()
    config = parse_config(args.config)

    # assert the data type parameters
    if args.trips == None and args.status_changes == None:
        print("One or both of the --status_changes or --trips arguments is required.")
        exit(1)
    
    # determine the MDS version to reference
    args.ref = args.ref or config["DEFAULT"]["ref"] or "master"
    print(f"Referencing MDS @ {args.ref}")

    # shortcut for loading from files
    if args.source:
        if args.status_changes:
            ingest(mds.STATUS_CHANGES, **vars(args))
        if args.trips:
            ingest(mds.TRIPS, **vars(args))
        # finished
        exit(0)

    # assert the time range parameters
    if args.start_time is None and args.end_time is None:
        arg_parser.print_help()
        exit(1)

    if (args.start_time is None or args.end_time is None) and args.duration is None:
        arg_parser.print_help()
        exit(1)

    # backfill mode if all 3 time parameters given
    args.backfill = all([args.start_time, args.end_time, args.duration])

    # parse into a valid range
    args.start_time, args.end_time = parse_time_range(args)

    # acquire the Provider registry and filter based on params
    if args.registry and Path(args.registry).is_file():
        print("Reading local provider registry...")
        registry = mds.providers.get_registry(file=args.registry)
    else:
        print("Downloading provider registry...")
        registry = mds.providers.get_registry(args.ref)

    print(f"Acquired registry: {', '.join(provider_names(registry))}")

    # filter the registry with cli args, and configure the providers that will be used
    providers = [p.configure(config, use_id=True)
                 for p in mds.providers.filter(registry, args.providers)]

    # parse any headers from the config to a dict
    for p in providers:
        headers = getattr(p, "headers", None)
        if headers and isinstance(headers, str):
            p.headers = json.loads(headers)

    # initialize an API client for these providers and configuration
    client = ProviderClient(providers)
    kwargs = dict(vars(args))

    # backfill mode
    if args.backfill:
        # do not forward with kwargs
        for key in ["backfill", "start_time", "end_time", "duration"]:
            del kwargs[key]
        if args.status_changes:
            backfill(mds.STATUS_CHANGES, client, args.start_time, args.end_time, args.duration, **kwargs)
        if args.trips:
            backfill(mds.TRIPS, client, args.start_time, args.end_time, args.duration, **kwargs)
    # simple request mode
    else:
        if args.status_changes:
            ingest(mds.STATUS_CHANGES, client=client, **kwargs)
        if args.trips:
            ingest(mds.TRIPS, client=client, **kwargs)
