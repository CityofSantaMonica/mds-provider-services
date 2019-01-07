"""
Load MDS provider data from a variety of sources into a database.
"""

import mds
from mds.db import ProviderDataLoader
from mds.providers import Provider
import os
from pathlib import Path


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


def load_data(datasource, record_type, **kwargs):
    """
    Insert data into a database from:

        - a dict of Provider to [list of data pages]
        - a list of JSON file paths

    If :db:, a `ProviderDataLoader` instance, is not given then obtain connection information from the Environment.
    """
    # db connection
    stage_first = int(kwargs.get("stage_first", False))
    on_conflict_update = bool(kwargs.get("on_conflict_update"))
    dbenv = { "stage_first": stage_first, "on_conflict_update": on_conflict_update, **db_env() }
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
            db.load_status_changes(src)
        elif record_type == mds.TRIPS:
            db.load_trips(src)
