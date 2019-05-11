"""
Load MDS provider data from a variety of sources into a database.
"""

import os
from pathlib import Path

from mds import STATUS_CHANGES, TRIPS, Database, Provider
from mds.versions import UnsupportedVersionError, Version

# default columns defining a unique record
STATUS_CHANGES_COLUMNS = ["provider_id", "device_id", "event_time", "event_type", "event_type_reason"]
TRIPS_COLUMNS = ["provider_id", "trip_id"]


def _prepare_conflict_update(columns, mds_version=None):
    """
    Create a tuple for generating an ON CONFLICT UPDATE statement.
    """
    mds_version = mds_version or Version.mds_lower()
    if mds_version.unsupported:
        raise UnsupportedVersionError(mds_version)

    if columns and len(columns) > 0:
        condition = f"({columns if isinstance(columns, str) else ', '.join(columns)})"
    else:
        raise TypeError("Columns are required.")

    return condition, version


def env():
    """
    Gets the database configuration out of the Environment. Fails if any values are missing.

    Returns dict { user, password, db, host, port }
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

    return dict(user=user, password=password, db=db, host=host, port=port)


def status_changes_conflict_update(columns, mds_version=None):
    """
    Create a tuple for generating the status_changes ON CONFLICT UPDATE statement.
    """
    condition, mds_version = _prepare_conflict_update(columns, mds_version)

    actions = {
        "event_type": "cast(EXCLUDED.event_type as event_types)",
        "event_type_reason": "cast(EXCLUDED.event_type_reason as event_type_reasons)",
        "event_location": "cast(EXCLUDED.event_location as jsonb)",
        "battery_pct": "EXCLUDED.battery_pct",
        "sequence_id": "EXCLUDED.sequence_id"
    }

    if mds_version < Version("0.3.0"):
        actions["associated_trips"] = "cast(EXCLUDED.associated_trips as uuid[])"
    else:
        actions["associated_trip"] = "cast(EXCLUDED.associated_trip as uuid)"

    return condition, actions


def trips_conflict_update(columns, mds_version=None):
    """
    Create a tuple for generating the trips ON CONFLICT UPDATE statement.
    """
    condition, _ = _prepare_conflict_update(columns, mds_version)

    actions = {
        "trip_duration": "EXCLUDED.trip_duration",
        "trip_distance": "EXCLUDED.trip_distance",
        "route": "cast(EXCLUDED.route as jsonb)",
        "accuracy": "EXCLUDED.accuracy",
        "start_time": "EXCLUDED.start_time",
        "end_time": "EXCLUDED.end_time",
        "parking_verification_url": "EXCLUDED.parking_verification_url",
        "standard_cost": "EXCLUDED.standard_cost",
        "actual_cost": "EXCLUDED.actual_cost",
        "sequence_id": "EXCLUDED.sequence_id"
    }

    return condition, actions


def load(datasource, record_type, **kwargs):
    """
    Load data into a database.
    """
    columns = kwargs.pop("columns", STATUS_CHANGES_COLUMNS if record_type == STATUS_CHANGES else TRIPS_COLUMNS)
    mds_version = Version(kwargs.pop("mds_version", Version.mds_lower()))
    stage_first = int(kwargs.pop("stage_first", True))

    _env = dict(stage_first=stage_first, **env())
    db = kwargs.get("db", Database(**_env))

    print(f"Loading {record_type}")

    conflict_update = bool(kwargs.get("on_conflict_update"))
    _kwargs = dict(table=record_type, on_conflict_update=conflict_update, drop_duplicates=columns)

    if record_type == STATUS_CHANGES:
        _kwargs["on_conflict_update"] = status_changes_conflict_update(columns, mds_version) if conflict_update else None
        db.load_status_changes(datasource, **_kwargs)
    elif record_type == TRIPS:
        _kwargs["on_conflict_update"] = trips_conflict_update(columns, mds_version) if conflict_update else None
        db.load_trips(datasource, **_kwargs)
    else:
        raise ValueError("Invalid record_type:", record_type)
