"""
Acquire MDS provider data from a variety of sources as in-memory objects.
"""

import os
from pathlib import Path

from mds import STATUS_CHANGES, TRIPS


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


def provider_names(providers):
    """
    Returns the names of the given Provider instances.
    """
    return [p.provider_name for p in providers]


def acquire_data(record_type, **kwargs):
    """
    Acquire provider data of type :record_type: as in-memory objects.
    """
    if kwargs.get("source"):
        datasource = expand_files(kwargs.get("source"), record_type)
        print(f"Loading from {datasource}")
        return datasource

    start_time = kwargs.get("start_time")
    end_time = kwargs.get("end_time")
    client = kwargs.get("client")
    paging = not kwargs.get("no_paging")
    rate_limit = kwargs.get("rate_limit")
    bbox = kwargs.get("bbox")
    providers = ", ".join(provider_names(client.providers))

    print(f"Requesting {record_type} from {providers}")
    print(f"Time range: {start_time.isoformat()} to {end_time.isoformat()}")

    if record_type == mds.STATUS_CHANGES:
        datasource = client.get_status_changes(
            start_time=start_time,
            end_time=end_time,
            bbox=bbox,
            paging=paging,
            rate_limit=rate_limit
        )
    elif record_type == mds.TRIPS:
        datasource = client.get_trips(
            device_id=kwargs.get("device_id"),
            vehicle_id=kwargs.get("vehicle_id"),
            start_time=start_time,
            end_time=end_time,
            bbox=bbox,
            paging=paging,
            rate_limit=rate_limit
        )

    return datasource
