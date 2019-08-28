"""
Helper functions for shared functionality.
"""

import datetime
import pathlib

import mds


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
        print("Reading configuration file:", config_path)
        return mds.ConfigFile(config_path, provider).dump()
    elif pathlib.Path("./config.json").exists():
        print("Found configuration file, reading...")
        return mds.ConfigFile("./config.json", provider).dump()
    else:
        print("No configuration file found.")
        return {}


def get_data(record_type, **kwargs):
    """
    Get provider data as in-memory objects.
    """
    if kwargs.get("source"):
        source = kwargs.get("source")
        print(f"Reading {record_type} from {source}")
        payloads = mds.DataFile(record_type, source).load_payloads()
        return payloads

    # required for API calls
    client = kwargs.pop("client")
    start_time = kwargs.pop("start_time")
    end_time = kwargs.pop("end_time")

    paging = not kwargs.get("no_paging")
    rate_limit = kwargs.get("rate_limit")
    version = kwargs.get("version")

    # package up for API requests
    _kwargs = dict(paging=paging, rate_limit=rate_limit)

    print(f"Requesting {record_type} from {client.provider.provider_name}")
    print(f"Time range: {start_time.isoformat()} to {end_time.isoformat()}")

    if record_type == mds.STATUS_CHANGES:
        _kwargs["start_time"] = start_time
        _kwargs["end_time"] = end_time
    elif record_type == mds.TRIPS:
        _kwargs["device_id"] = kwargs.get("device_id")
        _kwargs["vehicle_id"] = kwargs.get("vehicle_id")

        if version < mds.Version("0.3.0"):
            _kwargs["start_time"] = start_time
            _kwargs["end_time"] = end_time
        else:
            _kwargs["min_end_time"] = start_time
            _kwargs["max_end_time"] = end_time

    return client.get(record_type, **_kwargs)


def parse_time_range(version, **kwargs):
    """
    Returns a valid range tuple (start_time, end_time) given some mix of:

    * start_time: datetime
    * end_time: datetime
    * duration: numeric seconds

    If both start_time and end_time are given, use those. Otherwise, compute from duration.
    """
    decoder = mds.encoding.TimestampDecoder(version=version)

    if "start_time" in kwargs and "end_time" in kwargs:
        start_time, end_time = decoder.decode(kwargs["start_time"]), decoder.decode(kwargs["end_time"])
        return (start_time, end_time) if start_time <= end_time else (end_time, start_time)

    duration = datetime.timedelta(seconds=kwargs["duration"])

    if "start_time" in kwargs:
        start_time = decoder.decode(kwargs["start_time"])
        return start_time, start_time + duration

    if "end_time" in kwargs:
        end_time = decoder.decode(kwargs["end_time"])
        return end_time - duration, end_time
