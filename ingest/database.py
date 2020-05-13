"""
Load MDS provider data from a variety of sources into a database.
"""

import os

import mds

import common


# default columns defining a unique record
COLUMNS = {
    mds.STATUS_CHANGES: ["provider_id", "device_id", "event_time", "event_type", "event_type_reason"],
    mds.TRIPS: ["provider_id", "trip_id"],
    mds.VEHICLES: ["provider_id", "device_id", "last_updated"]
}
COLUMNS[mds.EVENTS] = COLUMNS[mds.STATUS_CHANGES]

# default ON CONFLICT UPDATE actions
UPDATE_ACTIONS = {
    mds.STATUS_CHANGES: {
        "event_type": "cast(EXCLUDED.event_type as event_types)",
        "event_type_reason": "cast(EXCLUDED.event_type_reason as event_type_reasons)",
        "event_location": "cast(EXCLUDED.event_location as jsonb)",
        "battery_pct": "EXCLUDED.battery_pct",
        "associated_trip": "cast(EXCLUDED.associated_trip as uuid)",
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
        "currency": "EXCLUDED.currency",
        "sequence_id": "EXCLUDED.sequence_id"
    },
    mds.VEHICLES: {
        "last_event_time": "EXCLUDED.last_event_time",
        "last_event_type": "cast(EXCLUDED.last_event_type as event_types)",
        "last_event_type_reason": "cast(EXCLUDED.last_event_type_reason as event_type_reasons)",
        "last_event_location": "cast(EXCLUDED.last_event_location as jsonb)",
        "current_location": "cast(EXCLUDED.current_location as jsonb)",
        "battery_pct": "EXCLUDED.battery_pct",
        "ttl": "EXCLUDED.ttl",
        "sequence_id": "EXCLUDED.sequence_id"
    }
}
UPDATE_ACTIONS[mds.EVENTS] = UPDATE_ACTIONS[mds.STATUS_CHANGES]


def conflict_update_condition(columns):
    """
    Create the (condition) portion of the "ON CONFLICT (condition) DO UPDATE (actions)" statement.
    """
    if columns and len(columns) > 0:
        return f"({columns if isinstance(columns, str) else ', '.join(columns)})"
    else:
        raise TypeError("Columns are required.")


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


def load(datasource, record_type, **kwargs):
    """
    Load data into a database.
    """
    print(f"Loading {record_type}")

    columns = kwargs.pop("columns", [])
    if len(columns) == 0:
        columns = COLUMNS[record_type]

    actions = kwargs.pop("update_actions", [])

    if len(actions) == 1 and actions[0] is True:
        # flag-only option, use defaults
        actions = UPDATE_ACTIONS[record_type]
    elif len(actions) > 1:
        # convert action tuples to dict, filtering any flag-only options
        actions = dict(filter(lambda x: x is not True, actions))

    version = mds.Version(kwargs.pop("version", common.default_version))
    stage_first = int(kwargs.pop("stage_first", True))

    db_config = dict(stage_first=stage_first, version=version, **env())
    db = kwargs.get("db", mds.Database(**db_config))

    load_config = dict(table=record_type, drop_duplicates=columns)
    if len(actions) > 0:
        load_config["on_conflict_update"] = conflict_update_condition(columns), actions

    if record_type == mds.EVENTS:
        db.load_events(datasource, **load_config)
    elif record_type == mds.STATUS_CHANGES:
        db.load_status_changes(datasource, **load_config)
    elif record_type == mds.TRIPS:
        db.load_trips(datasource, **load_config)
    elif record_type == mds.VEHICLES:
        db.load_vehicles(datasource, **load_config)
