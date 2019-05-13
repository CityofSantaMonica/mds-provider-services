"""
Command-line interface implementing various MDS Provider data ingestion flows, including:

  - request data from one or more Provider endpoints
  - validate data against any version of the schema
  - save data to the filesystem and/or load it into Postgres

All fully customizable through extensive parameterization and configuration options.
"""

import argparse
from configparser import ConfigParser
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import sys
import time

from mds.api import Client
from mds.encoding import TimestampDecoder
from mds.files import ConfigFile
from mds.providers import Provider
from mds.schemas import STATUS_CHANGES, TRIPS
from mds.versions import UnsupportedVersionError, Version

import database
import ingest
import validation


def setup_cli():
    """
    Create the cli argument interface, and parses incoming args.

    Returns a tuple:
        - the argument parser
        - the parsed args
    """
    parser = argparse.ArgumentParser(description="Ingest MDS data from various sources.")

    parser.add_argument(
        "provider",
        type=str,
        help="The name or identifier of the provider to query."
    )

    parser.add_argument(
        "--auth_type",
        type=str,
        default="Bearer",
        help="The type used for the Authorization header for requests to the provider\
        (e.g. Basic, Bearer)."
    )
    parser.add_argument(
        "--columns",
        type=str,
        nargs="+",
        default=[],
        help="One or more column names determining a unique record.\
        Used to drop duplicates in incoming data and detect conflicts with existing records.\
        NOTE: the program does not differentiate between --columns for --status_changes or --trips."
    )
    parser.add_argument(
        "--config",
        type=str,
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
        "-H",
        "--header",
        dest="headers",
        action="append",
        type=lambda kv: (kv.split(":", 1)[0].strip(), kv.split(":", 1)[1].strip()),
        default=[],
        help="One or more 'Header: value' combinations, sent with each request."
    )
    parser.add_argument(
        "--no_load",
        action="store_true",
        help="Do not attempt to load the returned data."
    )
    parser.add_argument(
        "--no_paging",
        action="store_true",
        help="Return only the first page of data."
    )
    parser.add_argument(
        "--no_validate",
        action="store_true",
        help="Do not perform JSON Schema validation against the returned data."
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Write results to json files in this directory."
    )
    parser.add_argument(
        "--rate_limit",
        type=int,
        default=0,
        help="Number of seconds to pause between paging requests to a given endpoint."
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
        help="False to append records directly to the data table.\
        True to stage in a temp table before UPSERT to the data table.\
        Int to increase randomness of the temp table name."
    )
    parser.add_argument(
        "--start_time",
        type=str,
        help="The beginning of the time query range for this request.\
        Should be either numeric Unix time or ISO-8601 datetime format.\
        At least one of end_time or start_time is required."
    )
    parser.add_argument(
        "--status_changes",
        action="store_true",
        help="Request status changes.\
        At least one of --status_changes or --trips is required."
    )
    parser.add_argument(
        "--trips",
        action="store_true",
        help="Request trips.\
        At least one of --status_changes or --trips is required."
    )
    parser.add_argument(
        "-U",
        "--on_conflict_update",
        action="append",
        const=True,
        default=[],
        dest="update_actions",
        nargs="?",
        type=lambda kv: (kv.split(":", 1)[0].strip(), kv.split(":", 1)[1].strip()),
        help="Perform an UPDATE when incoming data conflicts with existing database records.\
        Specify one or more 'column_name: EXCLUDED.value' to build an ON CONFLICT UPDATE statement.\
        NOTE: the program does not differentiate between --on_conflict_update for --status_changes or --trips."
    )
    parser.add_argument(
        "--vehicle_id",
        type=str,
        help="The vehicle_id to obtain results for. Only applies to --trips."
    )
    parser.add_argument(
        "--version",
        type=lambda v: Version(v),
        default=Version.mds_lower(),
        help="The release version at which to reference MDS, e.g. 0.3.1"
    )

    return parser, parser.parse_args()


def parse_time_range(cli):
    """
    Returns a valid range tuple (start_time, end_time) given an object with some mix of:

    * start_time
    * end_time
    * duration

    If both start_time and end_time are present, use those. Otherwise, compute from duration.
    """
    decoder = TimestampDecoder(version=cli.version)

    if cli.start_time is not None and cli.end_time is not None:
        start_time, end_time = decoder.decode(cli.start_time), decoder.decode(cli.end_time)
        return (start_time, end_time) if start_time <= end_time else (end_time, start_time)

    if cli.start_time is not None:
        start_time = decoder.decode(cli.start_time)
        return start_time, start_time + timedelta(seconds=cli.duration)

    if cli.end_time is not None:
        end_time = decoder.decode(cli.end_time)
        return end_time - timedelta(seconds=cli.duration), end_time


def count_seconds(ts):
    """
    Return the number of seconds since a given UNIX datetime.
    """
    return round((datetime.utcnow() - ts).total_seconds())


if __name__ == "__main__":
    now = datetime.utcnow()

    # command line args
    arg_parser, args = setup_cli()

    # assert the data type parameters
    if not (args.trips or args.status_changes):
        print("One or both of the --status_changes or --trips flags is required. Run main.py --help for more information.")
        print("Exiting.")
        print()
        exit(1)

    print(f"Starting ingestion run: {now.isoformat()}")

    if args.config:
        print("Reading configuration file:", args.config)
        config = ConfigFile(args.config, args.provider).dump()
    elif Path("./config.json").exists():
        print("Found configuration file, reading...")
        config = ConfigFile("./config.json", args.provider).dump()
    else:
        print("No configuration file found.")
        config = {}

    # assert the version parameter
    args.version = Version(config.pop("version", args.version))
    if args.version.unsupported:
        raise UnsupportedVersionError(args.version)

    # shortcut for loading from files
    if args.source:
        if args.status_changes:
            ingest.run(STATUS_CHANGES, **vars(args))
        if args.trips:
            ingest.run(TRIPS, **vars(args))
        # finished
        print(f"Finished ingestion ({count_seconds(now)}s)")
        exit(0)

    # assert the time range parameters
    if args.start_time is None and args.end_time is None:
        print("One or both of --end_time or --start_time is required. Run main.py --help for more information.")
        print("Exiting.")
        print()
        exit(1)

    if (args.start_time is None or args.end_time is None) and args.duration is None:
        print("With only one of --end_time or --start_time, --duration is required. Run main.py --help for more information.")
        print("Exiting.")
        print()
        exit(1)

    # backfill mode if all 3 time parameters given
    backfill = all([args.start_time, args.end_time, args.duration])

    # parse into a valid range
    args.start_time, args.end_time = parse_time_range(args)

    print(f"Referencing MDS @ {args.version}")

    # acquire the Provider instance
    if args.registry and Path(args.registry).is_file():
        print("Reading local provider registry...")
        provider = Provider(args.provider, path=args.registry, **config)
    else:
        print("Downloading provider registry...")
        provider = Provider(args.provider, version=args.version, **config)

    print(f"Provider '{provider.provider_name}' is configured.")

    # initialize an API client for the provider
    client = Client(provider, version=args.version)
    kwargs = dict(client=client, **vars(args))

    if backfill:
        if args.status_changes:
            ingest.backfill(STATUS_CHANGES, **kwargs)
        if args.trips:
            ingest.backfill(TRIPS, **kwargs)

    else:
        if args.status_changes:
            ingest.run(STATUS_CHANGES, **kwargs)
        if args.trips:
            ingest.run(TRIPS, **kwargs)

    print(f"Finished ingestion ({count_seconds(now)}s)")
