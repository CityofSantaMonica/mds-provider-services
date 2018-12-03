from mds.db.load import data_engine
import os
import pandas


def parse_db_env():
    """
    Gets the required database configuration out of the Environment.

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

conn = data_engine(**parse_db_env())


class Availability:
    """
    Represents a query of the availability view for a particular provider.
    """

    DEFAULT_TABLE = "availability"

    def __init__(self, start, end, provider_name=None, vehicle_types=None, start_types=None, end_types=None, table=DEFAULT_TABLE, local=False, debug=False):
        """
        Initialize a new `Availability` query with the given parameters.

        Required positional arguments:

        :start: A python datetime, ISO8601 datetime string, or Unix timestamp for the beginning of the interval.

        :end: A python datetime, ISO8601 datetime string, or Unix timestamp for the end of the interval.

        Supported optional keyword arguments:

        :provider_name: The name of a provider, as found in the providers registry.

        :vehicle_types: vehicle_type or list of vehicle_type to further restrict the query.

        :start_types: event_type or list of event_type to restrict the `start_event_type` (e.g. `available`).

        :end_types: event_type or list of event_type to restrict the `end_event_type` (e.g. `available`).

        :table: Name of the table or view containing the availability records.

        :local: False (default) to query the Unix time data columns; True to query the local time columns.

        :debug: False (default) to supress debug messages; True to print debug messages.
        """
        self.start = start
        self.end = end
        self.provider_name = provider_name
        self.vehicle_types = vehicle_types
        self.start_types = start_types
        self.end_types = end_types
        self.table = table
        self.local = local
        self.debug = debug

    def get(self, provider_name=None, vehicle_types=None, start_types=None, end_types=None, predicates=None):
        """
        Execute a query against the availability view.

        Supported optional keyword arguments:

        :provider_name: The name of a provider, as found in the providers registry.

        :vehicle_types: vehicle_type or list of vehicle_type to further restrict the query.

        :start_types: event_type or list of event_type to restrict the `start_event_type` (e.g. `available`).

        :end_types: event_type or list of event_type to restrict the `end_event_type` (e.g. `available`).

        :predicates: Additional predicates that will be ANDed to the WHERE clause (e.g `vehicle_id = '1234'`).

        :table: Name of the table or view containing the availability records.

        :returns: A `pandas.DataFrame` of events from the given provider, crossing this query's time range.
        """
        start_time = "start_time_local" if self.local else "start_time"
        end_time = "end_time_local" if self.local else "end_time"

        if predicates:
            predicates = [predicates] if not isinstance(predicates, list) else predicates
        else:
            predicates = []

        if provider_name or self.provider_name:
            predicates.append(f"provider_name = '{provider_name or self.provider_name}'")

        vts = "'::vehicle_types,'"
        vehicle_types = vehicle_types or self.vehicle_types
        if vehicle_types:
            if not isinstance(vehicle_types, list):
                vehicle_types = [vehicle_types]
            predicates.append(f"vehicle_type IN ('{vts.join(vehicle_types)}'::vehicle_types)")

        ets = "'::event_types,'"
        start_types = start_types or self.start_types
        end_types = end_types or self.end_types

        if start_types:
            if not isinstance(start_types, list):
                start_types = [start_types]
            predicates.append(f"start_event_type IN ('{ets.join(start_types)}'::event_types)")

        if end_types:
            if not isinstance(end_types, list):
                end_types = [end_types]
            predicates.append(f"end_event_type IN ('{ets.join(end_types)}'::event_types)")

        predicates = " AND ".join(predicates)

        sql = f"""
            SELECT
                *
            FROM
                {self.table}
            WHERE
                {predicates} AND
                (({start_time} <= '{self.start}' AND {end_time} > '{self.start}') OR
                ({start_time} < '{self.end}' AND {end_time} >= '{self.end}') OR
                ({start_time} >= '{self.start}' AND {end_time} <= '{self.end}') OR
                ({start_time} < '{self.end}' AND {end_time} IS NULL))
            ORDER BY
                {start_time}, {end_time}
            """

        if self.debug:
            print("Sending query:")
            print(sql)

        data = pandas.read_sql(sql, conn, index_col=None)

        if self.debug:
            print(f"Got {len(data)} results")

        return data
