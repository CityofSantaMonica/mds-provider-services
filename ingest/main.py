import argparse
from configparser import ConfigParser
from datetime import timedelta
import dateutil.parser
import json
import mds
from mds.api import ProviderClient
from mds.db import ProviderDataLoader
import mds.providers
from mds.schema.validation import ProviderDataValidator
import os


def parse_db_env():
    """
    Gets the required database configuration out of the Environment.

    Returns a tuple:
        - user
        - password
        - db_name
        - host
        - port
    """
    try:
        user, password = os.environ["MDS_USER"], os.environ["MDS_PASSWORD"]
    except:
        print("The MDS_USER or MDS_PASSWORD environment variables are not set. Exiting.")
        exit(1)

    try:
        db_name = os.environ["MDS_DB"]
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

    return user, password, db_name, host, port


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
        Should be either int Unix seconds or ISO-8061 datetime format.\
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
        "--ref",
        type=str,
        help="Git branch name, commit hash, or tag at which to reference MDS.\
        The default is `master`."
    )
    parser.add_argument(
        "--start_time",
        type=str,
        help="The beginning of the time query range for this request.\
        Should be either int Unix seconds or ISO-8061 datetime format.\
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
    def _to_datetime(input):
        """
        Helper to parse different textual representations into datetime
        """
        return dateutil.parser.parse(input)

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


def data_validation(data, validator):
    """
    Helper to validate data retrieved from APIs.
    """
    for provider, pages in data.items():
        print("Validating data from", provider.provider_name)

        # validate each page of data for this provider
        valid = True
        for payload in pages:
            for error in validator.validate(payload):
                print(error)
                valid = False

        # return a validation result per provider
        yield valid


def dump_payloads(output, payloads, datatype, start_time, end_time):
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


def ingest_status_changes(cli, client, db, start_time, end_time, paging, validating, loading):
    """
    Run the ingestion flow for Status Changes, using the configured :client: and the given params.
    """
    print("Requesting Status Changes")
    print(f"Time range: {start_time.isoformat()} to {end_time.isoformat()}")

    sc = client.get_status_changes(
        start_time=start_time,
        end_time=end_time,
        bbox=cli.bbox,
        paging=paging
    )

    if cli.output and os.path.exists(cli.output):
        print(f"Writing data files to {cli.output}")
        dump_payloads(cli.output, sc, mds.STATUS_CHANGES, start_time, end_time)

    if validating:
        print("Validating Status Changes")
        validator = ProviderDataValidator.StatusChanges(ref=ref)
        valid = all(data_validation(sc, validator))
    else:
        print("Skipping data validation")
        valid = True

    if loading and valid:
        for provider, payload in sc.items():
            print("Loading Status Changes for", provider.provider_name)
            db.load_status_changes(payload)

    print("Status Changes complete")


def ingest_trips(cli, client, db, start_time, end_time, paging, validating, loading):
    """
    Run the ingestion flow for Trips, using the configured :client: and the given params.
    """
    print("Requesting Trips")
    print(f"Time range: {start_time.timestamp()} to {end_time.timestamp()}")

    trips = client.get_trips(
        device_id=cli.device_id,
        vehicle_id=cli.vehicle_id,
        start_time=start_time,
        end_time=end_time,
        bbox=cli.bbox,
        paging=paging
    )

    if cli.output and os.path.exists(cli.output):
        print(f"Writing data files to {cli.output}")
        dump_payloads(cli.output, trips, mds.TRIPS, start_time, end_time)

    if validating:
        print("Validating Trips")
        validator = ProviderDataValidator.Trips(ref=ref)
        valid = all(data_validation(trips, validator))
    else:
        print("Skipping Trips validation.")
        valid = True

    if loading and valid:
        for provider, payload in trips.items():
            print("Loading Trips for", provider.provider_name)
            db.load_trips(payload)

    print("Trips complete")


if __name__ == "__main__":
    db = ProviderDataLoader(*parse_db_env())

    arg_parser, args = setup_cli()

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
    print(f"Requesting from providers: {provider_names(providers)}")

    # parse any headers from the config to a dict
    for p in providers:
        headers = getattr(p, "headers", None)
        if headers and isinstance(headers, str):
            p.headers = json.loads(headers)

    # initialize an API client for these providers and configuration
    client = ProviderClient(providers)
    paging = not args.no_paging
    validating = not args.no_validate
    loading = not args.no_load

    if args.status_changes:
        ingest_status_changes(args, client, db, start_time,
                              end_time, paging, validating, loading)

    if args.trips:
        ingest_trips(args, client, db, start_time,
                     end_time, paging, validating, loading)
