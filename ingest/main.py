"""
Command-line interface implementing various MDS Provider data ingestion flows, including:

  - request data from one or more Provider endpoints
  - validate data against any version of the schema
  - save data to the filesystem and/or load it into Postgres

All fully customizable through extensive parameterization and configuration options.
"""

import argparse
from configparser import ConfigParser
from datetime import datetime, timedelta, timezone
import dateutil.parser
import json
import mds
from mds.api import ProviderClient
from mds.db import ProviderDataLoader
import mds.providers
from mds.schema.validation import ProviderDataValidator
import os
from pathlib import Path
import re
import sys


def parse_db_env():
    """
    Gets the required database configuration out of the Environment.

    Returns dict:
        - user
        - password
        - db
        - host
        - port
    """
    try:
        user, password = os.environ["MDS_USER"], os.environ["MDS_PASSWORD"]
    except:
        print("The MDS_USER or MDS_PASSWORD environment variables are not set. Exiting.")
        exit(1)

    try:
        db = os.environ["MDS_DB"]
    except:
        print("The MDS_DB environment variable is not set. Exiting.")
        exit(1)

    try:
        host = os.environ["POSTGRES_HOSTNAME"]
    except:
        print("The POSTGRES_HOSTNAME environment variable is not set. Exiting.")
        exit(1)

    try:
        port = os.environ["POSTGRES_HOST_PORT"]
    except:
        port = 5432
        print("No POSTGRES_HOST_PORT environment variable set, defaulting to:", port)

    return { "user": user, "password": password, "db": db, "host": host, "port": port }


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
        help="Path to a provider configuration file to use.\
        The default is `.config`."
    )
    parser.add_argument(
        "--device_id",
        type=str,
        help="The device_id to obtain results for.\
        Only applies to --trips."
    )
    parser.add_argument(
        "--duration",
        type=int,
        help="Number of seconds; with --start_time or --end_time,\
        defines a time query range."
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
        help="Flag indicating paging through the response should *not* occur.\
        Return *only* the first page of data."
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
        "--providers",
        type=str,
        nargs="+",
        help="One or more provider names to query, separated by commas.\
        The default is to query all configured providers."
    )
    parser.add_argument(
        "--rate-limit",
        type=int,
        help="Number of seconds to pause between paging requests to a given endpoint."
    )
    parser.add_argument(
        "--ref",
        type=str,
        help="Git branch name, commit hash, or tag at which to reference MDS.\
        The default is `master`."
    )
    parser.add_argument(
        "--source",
        type=str,
        nargs="+",
        help="One or more paths to (directories containing) MDS Provider JSON file(s)"
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
        help="The vehicle_id to obtain results for.\
        Only applies to --trips."
    )

    return parser, parser.parse_args()


def parse_config(path):
    """
    Helper to parse a config file at :path:, which defaults to `.config`.
    """
    path = path or os.path.join(os.getcwd(), ".config")

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
            return datetime.fromtimestamp(int(data), timezone.utc)
        except:
            return dateutil.parser.parse(data)

    if args.start_time is not None and args.end_time is not None:
        return _to_datetime(args.start_time), _to_datetime(args.end_time)

    duration = int(args.duration)

    if args.start_time is not None:
        start_time = _to_datetime(args.start_time)
        return start_time, start_time + timedelta(seconds=duration)

    if args.end_time is not None:
        end_time = _to_datetime(args.end_time)
        return end_time - timedelta(seconds=duration), end_time


def provider_names(providers):
    """
    Returns the names of the :providers:, separated by commas.
    """
    return ", ".join([p.provider_name for p in providers])


def filter_providers(providers, names):
    """
    Filters the list of :providers: given one or more :names:.
    """
    if names is None or len(names) == 0:
        return providers

    if isinstance(names, str):
        names = [names]

    names = [n.lower() for n in names]

    return [p for p in providers if p.provider_name.lower() in names]


def expand_files(sources, record_type):
    """
    Return a list of all the files from a potentially mixed list of files and directories.
    Expands into the directories and includes their files in the output, as well as any input files.
    """
    # separate
    files = [Path(f) for f in sources if os.path.isfile(f) and record_type in f]
    dirs = [Path(d) for d in sources if os.path.isdir(d)]

    # expand and extend
    expanded = [f for ls in [Path(d).glob(f"*{record_type}*") for d in dirs] for f in ls]
    files.extend(expanded)

    return files


def acquire_data(record_type, cli, client, start_time, end_time, paging):
    """
    Obtains provider data as in-memory objects.
    """
    if cli.source:
        datasource = expand_files(cli.source, record_type)

        print(f"Loading from {datasource}")
    else:
        print(f"Requesting {record_type} from {provider_names(client.providers)}")
        print(f"Time range: {start_time.isoformat()} to {end_time.isoformat()}")

        if record_type == mds.STATUS_CHANGES:
            datasource = client.get_status_changes(
                start_time=start_time,
                end_time=end_time,
                bbox=cli.bbox,
                paging=paging,
                rate_limt=cli.rate_limit or 0
            )
        elif record_type == mds.TRIPS:
            datasource = client.get_trips(
                device_id=cli.device_id,
                vehicle_id=cli.vehicle_id,
                start_time=start_time,
                end_time=end_time,
                bbox=cli.bbox,
                paging=paging,
                rate_limt=cli.rate_limit or 0
            )

    return datasource


def output_data(output, payloads, datatype, start_time, end_time):
    """
    Write a the :payloads: dict (provider name => [data payload]) to json files in :output:.
    """
    def _file_name(output, provider, datatype, start_time, end_time):
        """
        Generate a filename from the given parameters.
        """
        fname = f"{provider}_{datatype}_{start_time.isoformat()}_{end_time.isoformat()}.json"
        return os.path.join(output, fname)

    for provider, payload in payloads.items():
        fname = _file_name(output, provider.provider_name,
                           datatype, start_time, end_time)
        with open(fname, "w") as f:
            json.dump(payload, f)


def validate_data(data, record_type, ref):
    """
    Helper to validate Provider data, which could be:

      - a dict of Provider => [pages]
      - a list of JSON file paths
    """
    print(f"Validating {record_type}")
    exceptions = [
        "is not a multiple of 1.0",
        "Payload error in links.prev",
        "Payload error in links.next",
        "Payload error in links.first",
        "Payload error in links.last",
        ".associated_trips: None is not of type 'array'",
        ".parking_verification_url: None is not of type 'string'"
    ]

    unexpected_prop_regx = re.compile("\('(\w+)' was unexpected\)")

    if record_type == mds.STATUS_CHANGES:
        validator = ProviderDataValidator.StatusChanges(ref=ref)
    elif record_type == mds.TRIPS:
        validator = ProviderDataValidator.Trips(ref=ref)
    else:
        raise ValueError(f"Invalid record_type: {record_type}")

    valid = {}
    for provider in data:
        if isinstance(provider, mds.providers.Provider):
            print("Validating data from", provider.provider_name)
            pages = data[provider]
        elif isinstance(provider, Path):
            print("Validating data from", provider)
            pages = json.load(provider.open("r"))
        elif isinstance(provider, str):
            print("Validating data from", provider)
            pages = json.load(open(provider, "r"))
        else:
            print("Skipping", provider)
            continue

        valid[provider] = []
        seen = 0
        passed = 0

        # validate each page of data for this provider
        for page in pages:
            invalid = False

            d = len(page.get("data", {}).get(record_type, []))
            seen += d

            for error in validator.validate(page):
                description = error.describe()
                # fail if this error message is not an exception
                if not any([ex in description for ex in exceptions]):
                    match = unexpected_prop_regx.search(description)
                    if match:
                        prop = match.group(1)
                        print("Removing unexpected property:", prop)
                        del error.instance[prop]
                    else:
                        print(description)
                        invalid = True

            if not invalid:
                passed += d
                valid[provider].append(page)

        print(f"Validated {seen} records ({passed} passed)")

    return valid


def load_data(datasource, record_type, db):
    """
    Load :record_type: data into :db: from :datasource:, which could be:

      - a dict of Provider => [pages]
      - a list of JSON file paths
    """
    for data in datasource:
        if isinstance(data, Path) or isinstance(data, str):
            # this is a file path
            print(f"Loading {record_type} from", data)
            src = data
        elif isinstance(data, mds.providers.Provider):
            # this is a dict payload from a Provider
            print(f"Loading {record_type} from", data.provider_name)
            src = datasource[data]

        if record_type == mds.STATUS_CHANGES:
            db.load_status_changes(src, stage_first=3)
        elif record_type == mds.TRIPS:
            db.load_trips(src, stage_first=3)


def ingest(record_type, ref, cli, client, db, start_time, end_time, paging, validating, loading):
    """
    Run the ingestion flow for the given :record_type:.
    """
    # acquire the data
    datasource = acquire_data(record_type, cli, client, start_time, end_time, paging)

    # do data validation
    if validating:
        valid = validate_data(datasource, record_type, ref)
    else:
        print("Skipping data validation")
        valid = datasource

    # output to files if needed
    if cli.output and os.path.exists(cli.output):
        print(f"Writing data files to {cli.output}")
        output_data(cli.output, datasource, record_type, start_time, end_time)

    # clean up before loading
    for k in [k for k in valid.keys()]:
        if not valid[k] or len(valid[k]) < 1:
            del valid[k]

    # do data loading
    if loading and len(valid) > 0:
        load_data(valid, record_type, db)

    print(f"{record_type} complete")


if __name__ == "__main__":
    now = datetime.utcnow().isoformat()
    print(f"Run time: {now}")

    # configuration
    db = ProviderDataLoader(**parse_db_env())
    arg_parser, args = setup_cli()
    paging = not args.no_paging
    validating = not args.no_validate
    loading = not args.no_load

    # shortcut for loading from files
    if args.source:
        if args.status_changes:
            ingest(record_type=mds.STATUS_CHANGES, ref=None, cli=args, client=None, db=db,
                   start_time=None, end_time=None, paging=paging, validating=validating,
                   loading=loading)
        if args.trips:
            ingest(record_type=mds.TRIPS, ref=None, cli=args, client=None, db=db,
                   start_time=None, end_time=None, paging=paging, validating=validating,
                   loading=loading)
        # finished
        sys.exit(0)

    # assert the time range parameters and parse a valid range
    if args.start_time is None and args.end_time is None:
        arg_parser.print_help()
        exit(1)

    if (args.start_time is None or args.end_time is None) and args.duration is None:
        arg_parser.print_help()
        exit(1)

    start_time, end_time = parse_time_range(args)

    # parse the config file
    config = parse_config(args.config)

    # determine the MDS version to reference
    ref = args.ref or config["DEFAULT"]["ref"] or "master"
    print(f"Referencing MDS @ {ref}")

    # download the Provider registry and filter based on params
    print("Downloading provider registry...")
    registry = mds.providers.get_registry(ref)

    print(f"Acquired registry: {provider_names(registry)}")

    # filter the registry with cli args, and configure the providers that will be used
    providers = [p.configure(config, use_id=True)
                 for p in filter_providers(registry, args.providers)]

    # parse any headers from the config to a dict
    for p in providers:
        headers = getattr(p, "headers", None)
        if headers and isinstance(headers, str):
            p.headers = json.loads(headers)

    # initialize an API client for these providers and configuration
    client = ProviderClient(providers)

    if args.status_changes:
        ingest(mds.STATUS_CHANGES, ref, args, client, db, start_time,
               end_time, paging, validating, loading)

    if args.trips:
        ingest(mds.TRIPS, ref, args, client, db, start_time,
               end_time, paging, validating, loading)
