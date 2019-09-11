"""
Helper functions for shared functionality.
"""

import datetime
import pathlib

import mds


default_version = mds.Version("0.3.2")


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
    api_kwargs = dict(paging=paging, rate_limit=rate_limit)

    print(f"Requesting {record_type} from {client.provider.provider_name}")
    print(f"Time range: {start_time.isoformat()} to {end_time.isoformat()}")

    if record_type == mds.STATUS_CHANGES:
        api_kwargs["start_time"] = start_time
        api_kwargs["end_time"] = end_time
    elif record_type == mds.TRIPS:
        api_kwargs["device_id"] = kwargs.get("device_id")
        api_kwargs["vehicle_id"] = kwargs.get("vehicle_id")

        if version < mds.Version("0.3.0"):
            api_kwargs["start_time"] = start_time
            api_kwargs["end_time"] = end_time
        else:
            api_kwargs["min_end_time"] = start_time
            api_kwargs["max_end_time"] = end_time

    return client.get(record_type, **api_kwargs)


def parse_time_range(**kwargs):
    """
    Returns a valid range tuple (start_time, end_time) given some mix of:

    * start_time: datetime
    * end_time: datetime
    * duration: numeric seconds

    If both start_time and end_time are given, use those. Otherwise, compute from duration.
    """
    decoder = mds.encoding.TimestampDecoder(version=kwargs["version"])

    if "start_time" in kwargs and "end_time" in kwargs and kwargs["start_time"] and kwargs["end_time"]:
        try:
            start_time, end_time = decoder.decode(int(kwargs["start_time"])), decoder.decode(int(kwargs["end_time"]))
        except ValueError:
            start_time, end_time = decoder.decode(kwargs["start_time"]), decoder.decode(kwargs["end_time"])
        return (start_time, end_time) if start_time <= end_time else (end_time, start_time)

    duration = datetime.timedelta(seconds=kwargs["duration"])

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
