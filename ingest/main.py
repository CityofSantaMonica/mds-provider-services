"""
Command-line interface implementing various MDS Provider data ingestion flows, including:

  - request data from one or more Provider endpoints
  - validate data against any version of the schema
  - save data to the filesystem
  - load data into Postgres

All fully customizable through extensive parameterization and configuration options.
"""

import datetime
import pathlib
import time

import mds

import common
import database
import validation


def setup_cli():
    """
    Create the cli argument interface, and parses incoming args.

    Returns a tuple:
        - the argument parser
        - the parsed args
    """
    parser = common.setup_cli(description="Ingest MDS data from various sources.")

    parser.add_argument(
        "provider",
        type=str,
        help="The name or identifier of the provider to query."
    )

    parser.add_argument(
        "--columns",
        type=str,
        nargs="+",
        default=[],
        help="One or more column names determining a unique record.\
        Used to drop duplicates in incoming data and detect conflicts with existing records.\
        Columns are reused if multiple record types are requested (e.g. --status_changes and --trips).\
        Make a distinct request per record type to overcome this limitation."
    )

    parser.add_argument(
        "--device_id",
        type=str,
        help="The device_id to obtain results for. Only valid for --trips and version < 0.4.0."
    )

    parser.add_argument(
        "--duration",
        type=int,
        help="Number of seconds; with one of --start_time or --end_time, defines a time query range.\
        For version < 0.4.0, time query ranges are valid for --status_changes and --trips.\
        For version >= 0.4.0, time query ranges are only valid for --events."
    )

    parser.add_argument(
        "--end_time",
        type=str,
        help="The end of the time query range for this request.\
        Should be either numeric Unix time or ISO-8601 datetime format.\
        For version < 0.4.0 at least one of end_time or start_time and duration is required.\
        For version >= 0.4.0 end_time is required for all but --vehicles."
    )

    parser.add_argument(
        "--events",
        action="store_true",
        help="Request events.\
        At least one of --events, --status_changes, --trips, or --vehicles is required."
    )

    parser.add_argument(
        "--no_load",
        action="store_true",
        help="Do not attempt to load the returned data into a database."
    )

    parser.add_argument(
        "--no_paging",
        action="store_true",
        help="Return only the first page of data.\
        For version >= 0.4.1, has no effect when requesting --status_changes or --trips."
    )

    parser.add_argument(
        "--no_validate",
        action="store_true",
        help="Do not perform JSON Schema validation against the returned data."
    )

    parser.add_argument(
        "--rate_limit",
        type=int,
        default=0,
        help="Number of seconds to pause between paging requests to a given endpoint.\
        For version >= 0.4.1, has no effect when requesting --status_changes or --trips."
    )

    parser.add_argument(
        "--registry",
        type=str,
        help="Path to a providers.csv registry file to use instead of downloading from GitHub."
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
        For version < 0.4.0, at least one of --end_time or --start_time and --duration is required.\
        For version >= 0.4.0, only valid for --events."
    )

    parser.add_argument(
        "--status_changes",
        action="store_true",
        help="Request status changes.\
        At least one of --events, --status_changes, --trips, or --vehicles is required."
    )

    parser.add_argument(
        "--trips",
        action="store_true",
        help="Request trips.\
        At least one of --events, --status_changes, --trips, or --vehicles is required."
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
        Conflict actions are reused if multiple record types are requested (e.g. --status_changes and --trips).\
        Make a distinct request per record type to overcome this limitation."
    )

    parser.add_argument(
        "--vehicle_id",
        type=str,
        help="The vehicle_id to obtain results for. Only valid for --trips and version < 0.4.0."
    )

    parser.add_argument(
        "--vehicles",
        action="store_true",
        help="Request vehicles.\
        At least one of --events, --status_changes, --trips, or --vehicles is required."
    )

    return parser, parser.parse_args()


def backfill(record_type, **kwargs):
    """
    Step backwards from end_time to start_time, running the ingestion flow in sliding blocks of size duration.

    Subsequent blocks overlap the previous block by duration/2 seconds.
    Buffers on both ends ensure events starting or ending near a time boundary are captured.

    For example:

    start_time=datetime(2018,12,31,0,0,0), end_time=datetime(2019,1,1,0,0,0), duration=21600

    Results in the following backfill requests:

    * 2018-12-31T21:00:00 to 2019-01-01T03:00:00
    * 2018-12-31T18:00:00 to 2018-12-31T00:00:00
    * 2018-12-31T15:00:00 to 2018-12-31T21:00:00
    * 2018-12-31T12:00:00 to 2018-12-31T18:00:00
    * 2018-12-31T09:00:00 to 2018-12-31T15:00:00
    * 2018-12-31T06:00:00 to 2018-12-31T12:00:00
    * 2018-12-31T03:00:00 to 2018-12-31T09:00:00
    * 2018-12-31T00:00:00 to 2018-12-31T06:00:00
    * 2018-12-30T21:00:00 to 2018-12-31T03:00:00

    Only valid for version < 0.4.0.
    """
    version = kwargs.pop("version")
    if version >= common.VERSION_040:
        raise ValueError("Backfill is only supported for version < 0.4.0.")

    kwargs["version"] = version
    kwargs["no_paging"] = False
    rate_limit = kwargs.get("rate_limit")

    duration = datetime.timedelta(seconds=kwargs.pop("duration"))
    offset = duration / 2
    end = kwargs.pop("end_time") + offset
    start = kwargs.pop("start_time")

    print(f"Beginning backfill: {start.isoformat()} to {end.isoformat()}, size {duration.total_seconds()}s")

    while end >= start:
        _start = end - duration
        ingest(record_type, **kwargs, start_time=_start, end_time=end)
        end = end - offset

        if rate_limit:
            time.sleep(rate_limit)


def ingest(record_type, **kwargs):
    """
    Run the ingestion flow:

    1. acquire data from files or API
    2. optionally validate data, filtering invalid records
    3. optionally write data to output files
    4. optionally load valid records into the database
    """
    version = mds.Version(kwargs.pop("version", common.default_version))
    version.raise_if_unsupported()

    datasource = common.get_data(record_type, **kwargs, version=version)
    data_key = mds.Schema(record_type).data_key

    # validation and filtering
    if not kwargs.pop("no_validate", False):
        print(f"Validating {record_type} @ {version}")

        valid, errors, removed = validation.validate(record_type, datasource, version=version)

        seen = sum([len(d["data"][data_key]) for d in datasource])
        passed = sum([len(v["data"][data_key]) for v in valid])
        failed = sum([len(r["data"][data_key]) for r in removed])

        print(f"{seen} records, {passed} passed, {failed} failed")
    else:
        print("Skipping data validation")
        valid = datasource
        removed = None

    # output to files if needed
    output = kwargs.pop("output", None)
    if output:
        f = mds.DataFile(record_type, output)
        f.dump_payloads(valid)
        if removed:
            f.dump_payloads(removed)

    # load to database
    loading = not kwargs.pop("no_load", False)
    if loading and len(valid) > 0:
        database.load(valid, record_type, **kwargs, version=version)
    else:
        print("Skipping data load")

    print(f"{record_type} complete")


if __name__ == "__main__":
    now = datetime.datetime.utcnow()

    # command line args
    arg_parser, args = setup_cli()

    # assert the data type parameters
    if not any([args.events, args.status_changes, args.trips, args.vehicles]):
        print("At least one of --events, --status_changes, --trips, or --vehicles is required.")
        print("Run main.py --help for more information.")
        print("Exiting.")
        print()
        exit(1)

    print(f"Starting ingestion run: {now.isoformat()}")

    config = common.get_config(args.provider, args.config)

    # assert the version parameter
    args.version = mds.Version(config.pop("version", args.version))
    args.version.raise_if_unsupported()

    print(f"Referencing MDS @ {args.version}")

    # shortcut for loading from files
    if args.source:
        if args.events:
            ingest(mds.EVENTS, **vars(args))
        if args.status_changes:
            ingest(mds.STATUS_CHANGES, **vars(args))
        if args.trips:
            ingest(mds.TRIPS, **vars(args))
        if args.vehicles:
            ingest(mds.VEHICLES, **vars(args))
        # finished
        print(f"Finished ingestion ({common.count_seconds(now)}s)")
        exit(0)

    # assert the time range parameters
    if args.version < common.VERSION_040:
        if args.start_time is None and args.end_time is None:
            print("One or both of --end_time or --start_time is required.")
            print("Run main.py --help for more information.")
            print("Exiting.")
            print()
            exit(1)
        elif (args.start_time is None or args.end_time is None) and args.duration is None:
            print("With only one of --end_time or --start_time, --duration is required.")
            print("Run main.py --help for more information.")
            print("Exiting.")
            print()
            exit(1)
    else:
        if args.events and (args.start_time is None or args.end_time is None) and args.duration is None:
            print("Requesting events requires a time query range.")
            print("With only one of --end_time or --start_time, --duration is required.")
            print("Run main.py --help for more information.")
            print("Exiting.")
            print()
            exit(1)
        elif any([args.status_changes, args.trips]) and args.end_time is None:
            print("Requesting status_changes or trips requires an --end_time.")
            print("Run main.py --help for more information.")
            print("Exiting.")
            print()
            exit(1)

    # backfill mode for version < 0.4.0 if all 3 time parameters given
    backfill_mode = all([args.version < common.VERSION_040, args.start_time, args.end_time, args.duration])

    # parse into a valid range
    args.start_time, args.end_time = common.parse_time_range(**vars(args))

    # acquire the Provider instance
    if args.registry and pathlib.Path(args.registry).is_file():
        print("Reading local provider registry...")
        provider = mds.Provider(args.provider, path=args.registry, **config)
    else:
        print("Downloading provider registry...")
        provider = mds.Provider(args.provider, ref=args.version, **config)

    print(f"Provider configured:")
    print(provider)

    # initialize an API client for the provider
    client = mds.Client(provider, version=args.version)
    kwargs = dict(client=client, **vars(args))

    if backfill_mode:
        if args.status_changes:
            backfill(mds.STATUS_CHANGES, **kwargs)
        if args.trips:
            backfill(mds.TRIPS, **kwargs)
    else:
        if args.events:
            ingest(mds.EVENTS, **kwargs)
        if args.status_changes:
            ingest(mds.STATUS_CHANGES, **kwargs)
        if args.trips:
            ingest(mds.TRIPS, **kwargs)
        if args.vehicles:
            ingest(mds.VEHICLES, **kwargs)

    print(f"Finished ingestion ({common.count_seconds(now)}s)")
