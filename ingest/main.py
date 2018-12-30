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
        help="Number of seconds to pause between paging requests to a given endpoint."
    )
    parser.add_argument(
        "--ref",
        type=str,
        help="Git branch name, commit hash, or tag at which to reference MDS.\
        The default is `master`."
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

    if args.start_time is not None:
        start_time = _to_datetime(args.start_time)
        return start_time, start_time + timedelta(seconds=args.duration)

    if args.end_time is not None:
        end_time = _to_datetime(args.end_time)
        return end_time - timedelta(seconds=args.duration), end_time


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


def acquire_data(record_type, **kwargs):
    """
    Obtains provider data of type :record_type: as in-memory objects.
    """
    source = kwargs.get("source")

    if source:
        datasource = expand_files(source, record_type)

        print(f"Loading from {datasource}")
    else:
        start_time = kwargs.get("start_time")
        end_time = kwargs.get("end_time")
        client = kwargs.get("client")
        paging = not kwargs.get("no_paging", False)
        rate_limit = kwargs.get("rate_limit", 0)
        bbox = kwargs.get("bbox")

        print(f"Requesting {record_type} from {provider_names(client.providers)}")
        print(f"Time range: {start_time.isoformat()} to {end_time.isoformat()}")

        if record_type == mds.STATUS_CHANGES:
            datasource = client.get_status_changes(
                start_time=start_time,
                end_time=end_time,
                bbox=bbox,
                paging=paging,
                rate_limt=rate_limit
            )
        elif record_type == mds.TRIPS:
            datasource = client.get_trips(
                device_id=kwargs.get("device_id"),
                vehicle_id=kwargs.get("vehicle_id"),
                start_time=start_time,
                end_time=end_time,
                bbox=bbox,
                paging=paging,
                rate_limt=rate_limit
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


def load_data(datasource, record_type, **kwargs):
    """
    Load :record_type: data from :datasource:, which could be:

      - a dict of Provider => [pages]
      - a list of JSON file paths
    """
    # db connection
    stage_first = kwargs.get("stage_first") or 3
    on_conflict_update = kwargs.get("on_conflict_update", False)
    dbenv = { "stage_first": stage_first, "on_conflict_update": on_conflict_update, **parse_db_env() }

    db = ProviderDataLoader(**dbenv)

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
            db.load_status_changes(src)
        elif record_type == mds.TRIPS:
            db.load_trips(src)


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

    validating = not kwargs.get("no_validate", False)
    if validating:
        ref = kwargs.get("ref")
        validated_data = validate_data(datasource, record_type, ref)
    else:
        print("Skipping data validation")
        validated_data = datasource

    # output to files if needed
    output = kwargs.get("output")
    if output and os.path.exists(output):
        print(f"Writing data files to {output}")
        start_time, end_time = kwargs.get("start_time"), kwargs.get("end_time")
        output_data(output, datasource, record_type, start_time, end_time)

    # clean up before loading
    for k in [k for k in validated_data.keys()]:
        if not validated_data[k] or len(validated_data[k]) < 1:
            del validated_data[k]

    loading = not kwargs.get("no_load", False)
    if loading and len(validated_data) > 0:
        load_data(validated_data, record_type, **kwargs)

    print(f"{record_type} complete")


if __name__ == "__main__":
    now = datetime.utcnow().isoformat()
    print(f"Run time: {now}")

    # configuration
    arg_parser, args = setup_cli()
    config = parse_config(args.config)

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

    # assert the time range parameters and parse a valid range
    if args.start_time is None and args.end_time is None:
        arg_parser.print_help()
        exit(1)

    if (args.start_time is None or args.end_time is None) and args.duration is None:
        arg_parser.print_help()
        exit(1)

    args.backfill = all([args.start_time, args.end_time, args.duration])

    args.start_time, args.end_time = parse_time_range(args)

    # acquire the Provider registry and filter based on params
    if args.registry and Path(args.registry).is_file():
        print("Reading local provider registry...")
        registry = mds.providers.get_registry(file=args.registry)
    else:
        print("Downloading provider registry...")
        registry = mds.providers.get_registry(args.ref)

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
    kwargs = vars(args)

    if args.backfill:
        start_time, end_time, duration = args.start_time, args.end_time, args.duration
        for key in ["start_time", "end_time", "duration"]:
            del kwargs[key]

        if args.status_changes:
            backfill(mds.STATUS_CHANGES, client, start_time, end_time, duration, **kwargs)
        if args.trips:
            backfill(mds.TRIPS, client, start_time, end_time, duration, **kwargs)
    else:
        if args.status_changes:
            ingest(mds.STATUS_CHANGES, client=client, **kwargs)
        if args.trips:
            ingest(mds.TRIPS, client=client, **kwargs)
