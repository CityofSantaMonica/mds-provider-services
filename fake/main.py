"""
Command-line interface implementing synthetic MDS Provider data generation:

  - custom geographic area, device inventory, time periods
  - generates complete "days" of service
  - saves data as JSON files to container volume

All fully customizable through extensive parameterization and configuration options.
"""

import argparse
import datetime
import json
import math
import os
import random
import time
import uuid


import mds
import mds.encoding
import mds.fake.util as util
import mds.geometry


def setup_cli():
    """
    Create the cli argument interface, and parses incoming args.

    Returns a tuple:
        - the argument parser
        - the parsed args
    """
    schema = mds.Schema(mds.TRIPS)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--boundary",
        type=str,
        help="Path to a data file with geographic bounds for the generated data. Overrides the MDS_BOUNDARY environment variable."
    )
    parser.add_argument(
        "--close",
        type=int,
        default=19,
        help="The hour of the day (24-hr format) that provider stops operations. Overrides --start and --end."
    )
    parser.add_argument(
        "--date_format",
        type=str,
        default="unix",
        help="Format for datetime input (to this CLI) and output (to stdout and files). Options:\
            - 'unix' for Unix timestamps (default)\
            - 'iso8601' for ISO 8601 format\
            - '<python format string>' for custom formats,\
            see https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior"
    )
    parser.add_argument(
        "--devices",
        type=int,
        help="The number of devices to model in the generated data"
    )
    parser.add_argument(
        "--end",
        type=str,
        help="The latest event in the generated data, in --date_format format"
    )
    parser.add_argument(
        "--inactivity",
        type=float,
        help="Describes the portion of the fleet that remains inactive."
    )
    parser.add_argument(
        "--open",
        type=int,
        default=7,
        help="The hour of the day (24-hr format) that provider begins operations. Overrides --start and --end."
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Path to a directory to write the resulting data file(s)"
    )
    parser.add_argument(
        "--propulsion_types",
        type=str,
        nargs="+",
        default=schema.propulsion_types,
        metavar="PROPULSION_TYPE",
        help="A list of propulsion_types to use for the generated data, e.g. '{}'".format(" ".join(schema.propulsion_types))
    )
    parser.add_argument(
        "--provider_name",
        type=str,
        help="The name of the fake mobility as a service provider"
    )
    parser.add_argument(
        "--provider_id",
        type=uuid.UUID,
        help="The ID of the fake mobility as a service provider"
    )
    parser.add_argument(
        "--start",
        type=str,
        help="The earliest event in the generated data, in --date_format format"
    )
    parser.add_argument(
        "--speed_mph",
        type=float,
        help="The average speed of devices in miles per hour. Cannot be used with --speed_ms"
    )
    parser.add_argument(
        "--speed_ms",
        type=float,
        help="The average speed of devices in meters per second. Always takes precedence"
    )
    parser.add_argument(
        "--vehicle_types",
        type=str,
        nargs="+",
        default=schema.vehicle_types,
        metavar="VEHICLE_TYPE",
        help="A list of vehicle_types to use for the generated data, e.g. '{}'".format(" ".join(schema.vehicle_types))
    )
    parser.add_argument(
        "--version",
        type=lambda v: mds.Version(v),
        default=mds.Version("0.2.1"),
        help="The release version at which to reference MDS, e.g. 0.3.1"
    )

    return parser, parser.parse_args()


if __name__ == "__main__":
    T0 = time.time()

    parser, args = setup_cli()
    print(f"Parsed args: {args}")

    try:
        boundary_file = args.boundary or os.environ["MDS_BOUNDARY"]
    except:
        print("A boundary file is required")
        exit(1)

    # collect the parameters for data generation
    provider_name = args.provider_name or f"provider_{util.random_string(3)}"
    provider_id = args.provider_id or uuid.uuid4()
    N = args.devices or random.randint(100, 500)

    encoder = mds.JsonEncoder(date_format=args.date_format, version=args.version)
    decoder = mds.TimestampDecoder(version=args.version)

    date_start = datetime.datetime.today()
    date_end = date_start

    if args.start:
        date_start = decoder.decode(args.start)
    if args.end:
        date_end = decoder.decode(args.end)

    hour_open = args.open
    hour_closed = args.close

    inactivity = random.uniform(0, 0.05) if args.inactivity is None else args.inactivity

    # convert speed to meters/second
    ONE_MPH_METERSSEC = 0.44704
    if args.speed_ms is not None:
        speed = args.speed_ms
    elif args.speed_mph is not None:
        speed = args.speed_mph * ONE_MPH_METERSSEC
    else:
        speed = random.uniform(8 * ONE_MPH_METERSSEC, 15 * ONE_MPH_METERSSEC)

    # setup a data directory
    outputdir = "data" if args.output is None else args.output
    os.makedirs(outputdir, exist_ok=True)

    print(f"Parsing boundary file: {boundary_file}")
    t1 = time.time()
    boundary = mds.geometry.parse_boundary(boundary_file, downloads=outputdir)
    print(f"Valid boundary: {boundary.is_valid} ({time.time() - t1} s)")

    gen = mds.fake.ProviderDataGenerator(
        boundary=boundary,
        speed=speed,
        vehicle_types=args.vehicle_types,
        propulsion_types=args.propulsion_types
    )

    print(f"Generating {N} devices for '{provider_name}'")
    t1 = time.time()
    devices = gen.devices(N, provider_name, provider_id)
    print(f"Generating devices complete ({time.time() - t1} s)")

    status_changes, trips = [], []

    print(
        f"Generating data from {encoder.encode(date_start)} to {encoder.encode(date_end)}")
    t1 = time.time()

    date = date_start
    while(date <= date_end):
        formatted_date = encoder.encode(date)
        print(f"Starting day: {formatted_date} (open hours {hour_open} to {hour_closed})")

        t2 = time.time()

        day_status_changes, day_trips = gen.service_day(devices, date, hour_open, hour_closed, inactivity)
        status_changes.extend(day_status_changes)
        trips.extend(day_trips)
        date = date + datetime.timedelta(days=1)

        print(f"Finished day: {formatted_date} ({time.time() - t2} s)")

    print(f"Finished generating data ({time.time() - t1} s)")

    if len(status_changes) > 0 or len(trips) > 0:
        print("Generating data files")
        t1 = time.time()

        trips_file = mds.DataFile(mds.TRIPS, outputdir)

        print("Writing trips")

        t2 = time.time()
        payload = gen.make_payload(trips=trips)
        trips_file.dump_payloads(payload)

        print(f"Finished ({time.time() - t2} s)")

        sc_file = mds.DataFile(mds.STATUS_CHANGES, outputdir)

        print("Writing status_changes")

        t2 = time.time()
        payload = gen.make_payload(status_changes=status_changes)
        sc_file.dump_payloads(payload)

        print(f"Finished ({time.time() - t2} s)")

        print(f"Finished generating data files ({time.time() - t1} s)")

    print(f"Fake data generation complete ({time.time() - T0} s)")
