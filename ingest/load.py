"""
Load MDS provider data from a variety of sources into a database.
"""

import os
from pathlib import Path

from mds import ProviderDataLoader, Provider, STATUS_CHANGES, TRIPS


def db_env():
    """
    Gets the database configuration out of the Environment. Fails if any values are missing.

    Returns dict:
        - user
        - password
        - db
        - host
        - port
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

    return { "user": user, "password": password, "db": db, "host": host, "port": port }


def status_changes_conflict_update():
    """
    Returns a tuple (condition, actions) for generating the status_changes ON CONFLICT UPDATE statement.
    """
    condition = "ON CONSTRAINT unique_event"
    actions = {
        "event_type": "cast(EXCLUDED.event_type as event_types)",
        "event_type_reason": "cast(EXCLUDED.event_type_reason as event_type_reasons)",
        "event_location": "cast(EXCLUDED.event_location as jsonb)",
        "battery_pct": "EXCLUDED.battery_pct",
        "associated_trips": "cast(EXCLUDED.associated_trips as uuid[])",
        "sequence_id": "EXCLUDED.sequence_id"
    }

    return (condition, actions)


def trips_conflict_update():
    """
    Returns a tuple (condition, actions) for generating the trips ON CONFLICT UPDATE statement.
    """
    condition = "ON CONSTRAINT pk_trips"
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

    return (condition, actions)


def load_data(datasource, record_type, **kwargs):
    """
    Insert data into a database from:

        - a dict of Provider to [list of data pages]
        - a list of JSON file paths

    If :db:, a `ProviderDataLoader` instance, is not given then obtain connection information from the Environment.
    """
    # db connection
    stage_first = int(kwargs.get("stage_first"))
    on_conflict_update = bool(kwargs.get("on_conflict_update"))
    dbenv = { "stage_first": stage_first, **db_env() }
    db = kwargs.get("db", ProviderDataLoader(**dbenv))

    for data in datasource:
        if isinstance(data, Path) or isinstance(data, str):
            # this is a file path
            print(f"Loading {record_type} from", data)
            src = data
        elif isinstance(data, Provider):
            # this is a dict payload from a Provider
            print(f"Loading {record_type} from", data.provider_name)
            src = datasource[data]

        if record_type == mds.STATUS_CHANGES:
            conflict_update = status_changes_conflict_update() if on_conflict_update else None
            db.load_status_changes(src, on_conflict_update=conflict_update)
        elif record_type == mds.TRIPS:
            conflict_update = trips_conflict_update() if on_conflict_update else None
            db.load_trips(src, on_conflict_update=conflict_update)
