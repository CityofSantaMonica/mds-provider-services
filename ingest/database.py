"""
Load MDS provider data from a variety of sources into a database.
"""

import os

import mds


# default columns defining a unique record
COLUMNS = {
    mds.STATUS_CHANGES: ["provider_id", "device_id", "event_time", "event_type", "event_type_reason"],
    mds.TRIPS: ["provider_id", "trip_id"]
}

# default ON CONFLICT UPDATE actions
UPDATE_ACTIONS = {
    mds.STATUS_CHANGES: {
        "event_type": "cast(EXCLUDED.event_type as event_types)",
        "event_type_reason": "cast(EXCLUDED.event_type_reason as event_type_reasons)",
        "event_location": "cast(EXCLUDED.event_location as jsonb)",
        "battery_pct": "EXCLUDED.battery_pct",
        "sequence_id": "EXCLUDED.sequence_id"
    },
    mds.TRIPS: {
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
}

def _prepare_conflict_update(columns, version=None):
    """
    Create a tuple for generating an ON CONFLICT UPDATE statement.
    """
    version = version or mds.Version.mds_lower()
    if version.unsupported:
        raise mds.UnsupportedVersionError(version)

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


def status_changes_conflict_update(columns, actions, version=None):
    """
    Create a tuple for generating the status_changes ON CONFLICT UPDATE statement.
    """
    condition, version = _prepare_conflict_update(columns, version)

    if version < mds.Version("0.3.0"):
        if "associated_trips" not in actions:
            actions["associated_trips"] = "cast(EXCLUDED.associated_trips as uuid[])"
    else:
        if "associated_trip" not in actions:
            actions["associated_trip"] = "cast(EXCLUDED.associated_trip as uuid)"

    return condition, actions


def trips_conflict_update(columns, actions, version=None):
    """
    Create a tuple for generating the trips ON CONFLICT UPDATE statement.
    """
    condition, _ = _prepare_conflict_update(columns, version)
    return condition, actions


def load(datasource, record_type, **kwargs):
    """
    Load data into a database.
    """
    columns = kwargs.pop("columns", [])
    if len(columns) == 0:
        columns = COLUMNS[record_type]

    _kwargs = dict(table=record_type, drop_duplicates=columns)

    actions = kwargs.pop("update_actions", [])

    if len(actions) == 1 and actions[0] is True:
        # flag-only option, use defaults
        actions = UPDATE_ACTIONS[record_type]
    elif len(actions) > 1:
        # convert action tuples to dict, filtering any flag-only options
        actions = dict(filter(lambda x: x is not True, actions))

    conflict_update = len(actions) > 0

    version = mds.Version(kwargs.pop("version", mds.Version.mds_lower()))
    stage_first = int(kwargs.pop("stage_first", True))

    _env = dict(stage_first=stage_first, **env())
    db = kwargs.get("db", mds.Database(**_env))

    print(f"Loading {record_type}")

    if record_type == mds.STATUS_CHANGES:
        _kwargs["on_conflict_update"] = status_changes_conflict_update(columns, actions, version) if conflict_update else None
        db.load_status_changes(datasource, **_kwargs)
    elif record_type == mds.TRIPS:
        _kwargs["on_conflict_update"] = trips_conflict_update(columns, actions, version) if conflict_update else None
        db.load_trips(datasource, **_kwargs)
