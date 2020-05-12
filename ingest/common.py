"""
Helper functions for shared functionality.
"""

import argparse
import datetime
import pathlib

import mds


DEFAULT_VERSION = mds.Version("0.3.2")
VERSION_040 = mds.Version("0.4.0")


def count_seconds(ts):
    """
    Return the number of seconds since a given UNIX datetime.
    """
    return round((datetime.datetime.utcnow() - ts).total_seconds())


def get_config(provider, config_path=None):
    """
    Obtain provider's configuration data from the given file path, or the default file path if None.
    """
    if config_path:
        return mds.ConfigFile(config_path, provider).dump()
    elif pathlib.Path("./config.json").exists():
        return mds.ConfigFile("./config.json", provider).dump()
    else:
        return {}


def get_data(record_type, **kwargs):
    """
    Get provider data as in-memory objects.
    """
    # shortcut reading from file source(s)
    if kwargs.get("source"):
        source = kwargs.get("source")
        print(f"Reading {record_type} from {source}")
        payloads = mds.DataFile(record_type, source).load_payloads()
        return payloads

    # required for API calls
    client = kwargs.pop("client")

    # dependent on version and record_type
    start_time = kwargs.get("start_time")
    end_time = kwargs.get("end_time")

    paging = not kwargs.get("no_paging")
    rate_limit = kwargs.get("rate_limit")
    version = kwargs.get("version", DEFAULT_VERSION)

    # package up for API requests
    api_kwargs = dict(paging=paging, rate_limit=rate_limit)

    print(f"Requesting {record_type} from {client.provider.provider_name}")
    if start_time and end_time:
        print(f"For time range: {start_time.isoformat()} to {end_time.isoformat()}")
    elif end_time:
        print(f"For time: {end_time.isoformat()}")

    if version < VERSION_040:
        if record_type == mds.STATUS_CHANGES:
            api_kwargs["start_time"] = start_time
            api_kwargs["end_time"] = end_time
        elif record_type == mds.TRIPS:
            api_kwargs["min_end_time"] = start_time
            api_kwargs["max_end_time"] = end_time
            api_kwargs["device_id"] = kwargs.get("device_id")
            api_kwargs["vehicle_id"] = kwargs.get("vehicle_id")
    else:
        if record_type == mds.EVENTS:
            api_kwargs["start_time"] = start_time
            api_kwargs["end_time"] = end_time
        elif record_type == mds.STATUS_CHANGES:
            api_kwargs["event_time"] = end_time
        elif record_type == mds.TRIPS:
            api_kwargs["end_time"] = end_time
        elif record_type == mds.VEHICLES:
            # currently no special query params for vehicles
            pass

    return client.get(record_type, **api_kwargs)


def parse_time_range(**kwargs):
    """
    Returns a valid range tuple (start_time, end_time) given some mix of:

    * start_time: datetime
    * end_time: datetime
    * duration: numeric seconds

    If both start_time and end_time are given, use those.
    Otherwise, compute the range from either side with duration. Duration is assumed 0 if not given.
    """
    decoder = mds.encoding.TimestampDecoder(version=kwargs["version"])

    if "start_time" in kwargs and "end_time" in kwargs and kwargs["start_time"] and kwargs["end_time"]:
        try:
            start_time, end_time = decoder.decode(int(kwargs["start_time"])), decoder.decode(int(kwargs["end_time"]))
        except ValueError:
            start_time, end_time = decoder.decode(kwargs["start_time"]), decoder.decode(kwargs["end_time"])
        return (start_time, end_time) if start_time <= end_time else (end_time, start_time)

    duration = datetime.timedelta(seconds=kwargs.get("duration", 0))

    if "start_time" in kwargs and kwargs["start_time"]:
        try:
            start_time = decoder.decode(int(kwargs["start_time"]))
        except ValueError:
            start_time = decoder.decode(kwargs["start_time"])
        return start_time, start_time + duration

    if "end_time" in kwargs and kwargs["end_time"]:
        try:
            end_time = decoder.decode(int(kwargs["end_time"]))
        except ValueError:
            end_time = decoder.decode(kwargs["end_time"])
        return end_time - duration, end_time


def setup_cli(**kwargs):
    """
    Set up the common command line arguments.

    Keyword arguments are passed to the ArgumentParser instance.

    Returns the ArgumentParser.
    """
    parser = argparse.ArgumentParser(**kwargs)

    parser.add_argument(
        "--auth_type",
        type=str,
        default="Bearer",
        help="The type used for the Authorization header for requests to the provider\
        (e.g. Basic, Bearer)."
    )

    parser.add_argument(
        "--config",
        type=str,
        help="Path to a provider configuration file to use."
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
        "--output",
        type=str,
        help="Write results to json files in this directory."
    )

    parser.add_argument(
        "--version",
        type=lambda v: mds.Version(v),
        default=DEFAULT_VERSION,
        help=f"The release version at which to reference MDS, e.g. {DEFAULT_VERSION}"
    )

    return parser
