"""
Acquire MDS provider payloads from a variety of sources as in-memory objects.
"""

import datetime
import time

import mds

import common
import database
import validation


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
    """
    kwargs["no_paging"] = False
    rate_limit = kwargs.get("rate_limit")

    duration = datetime.timedelta(seconds=kwargs.pop("duration"))
    offset = duration / 2
    end = kwargs.pop("end_time") + offset
    start = kwargs.pop("start_time")

    print(f"Beginning backfill: {start.isoformat()} to {end.isoformat()}, size {duration.total_seconds()}s")

    while end >= start:
        _start = end - duration
        run(record_type, **kwargs, start_time=_start, end_time=end)
        end = end - offset

        if rate_limit:
            time.sleep(rate_limit)


def run(record_type, **kwargs):
    """
    Run the ingestion flow:

    1. acquire data from files or API
    2. optionally validate data, filtering invalid records
    3. optionally write data to output files
    4. optionally load valid records into the database
    """
    version = mds.Version(kwargs.pop("version", mds.Version.mds_lower()))
    if version.unsupported:
        raise mds.UnsupportedVersionError(version)

    datasource = common.get_data(record_type, **kwargs, version=version)

    # output to files if needed
    output = kwargs.pop("output", None)
    if output:
        print(f"Writing data files to {output}")
        mds.DataFile(record_type, output).dump_payloads(datasource)

    if not kwargs.pop("no_validate", False):
        datasource = validation.filter(record_type, datasource, version=version)
    else:
        print("Skipping data validation")

    # load to database
    loading = not kwargs.pop("no_load", False)
    if loading and len(datasource) > 0:
        database.load(datasource, record_type, **kwargs, version=version)

    print(f"{record_type} complete")
