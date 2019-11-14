"""
Create time-bounded SQL queries for interval calculations like availability.
"""

import os

import mds
import pandas


def parse_db_env():
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


ENGINE = mds.db.data_engine(**parse_db_env())


class TimeQuery():
    """
    Represents a query over a time period.
    """

    def __init__(self, start, end, **kwargs):
        """
        Initialize a new `TimeQuery` with the given parameters.

        Required positional arguments:

        :start: A python datetime, ISO8601 datetime string, or Unix timestamp for the beginning of the interval.

        :end: A python datetime, ISO8601 datetime string, or Unix timestamp for the end of the interval.

        Supported optional keyword arguments:

        :cutoff: Maximum allowed length of a time-windowed event (e.g. availability window, trip), as int number of days.

        :engine: A `sqlalchemy.engine.Engine` representing a connection to the database.

        :source: Name of the table or view containing the source records. This is required either at initialization or query time.

        :provider_name: The name of a provider, as found in the providers registry.

        :vehicle_types: vehicle_type or list of vehicle_type to further restrict the query.

        :order_by: Column name(s) for the ORDER BY clause.

        :local: False (default) to query the Unix time data columns; True to query the local time columns.

        :debug: False (default) to supress debug messages; True to print debug messages.
        """
        if not start or not end:
            raise ValueError("Start and End are required.")

        self.start = start
        self.end = end
        self.cutoff = kwargs.get("cutoff", -1)
        self.engine = kwargs.get("engine")
        self.source = kwargs.get("source")
        self.provider_name = kwargs.get("provider_name")
        self.vehicle_types = kwargs.get("vehicle_types")
        self.order_by = kwargs.get("order_by", "")
        self.local = kwargs.get("local", False)
        self.debug = kwargs.get("debug", False)

    def prepare_sql(self, **kwargs):
        """
        Build the SQL statement for this TimeQuery.

        Supported optional keyword arguments:

        :cutoff: Maximum allowed length of a time-windowed event (e.g. availability window, trip), as int number of days.

        :order_by: Column name(s) for the ORDER BY clause.

        :predicates: Additional predicates that will be ANDed to the WHERE clause (e.g `vehicle_id = '1234'`).

        :provider_name: The name of a provider, as found in the providers registry.

        :source: Name of the table or view containing the source records. This is required either at initialization or query time.

        :vehicle_types: vehicle_type or list of vehicle_type to further restrict the query.
        """
        source = kwargs.get("source", self.source)
        if not source:
            raise ValueError("No source table or view specified.")

        cutoff = kwargs.get("cutoff", self.cutoff)
        start_time = "start_time_local" if self.local else "start_time"
        end_time = "end_time_local" if self.local else "end_time"

        predicates = kwargs.get("predicates", [])
        predicates = [predicates] if not isinstance(predicates, list) else predicates

        provider_name = kwargs.get("provider_name", self.provider_name)

        if provider_name:
            predicates.append(f"lower(provider_name) = lower('{provider_name}')")

        vts = "'::vehicle_types,'"
        vehicle_types = kwargs.get("vehicle_types", self.vehicle_types)
        if vehicle_types:
            if not isinstance(vehicle_types, list):
                vehicle_types = [vehicle_types]
            predicates.append(f"vehicle_type IN ('{vts.join(vehicle_types)}'::vehicle_types)")

        if len(predicates) > 0:
            predicates = " AND ".join(predicates) + " AND "
        else:
            predicates = ""

        timeranges = [
            f"({start_time} <= %(start)s AND {end_time} > %(start)s)",
            f"({start_time} < %(end)s AND {end_time} >= %(end)s)",
            f"({start_time} >= %(start)s AND {end_time} <= %(end)s)"
        ]

        if cutoff > 0:
            where = f"""
            {predicates}
                ({end_time} IS NULL AND {start_time} between %(start)s - '{cutoff} days'::interval and %(end)s OR
                (date_part('day', {end_time} - {start_time}) <= {cutoff} AND
                    ({" OR ".join(timeranges)})))
            """
        else:
            timeranges.append(f"({start_time} < %(end)s AND {end_time} IS NULL)")
            where = f"""
            {predicates}
                ({" OR ".join(timeranges)})
            """

        order_by = kwargs.get("order_by", self.order_by)
        if order_by:
            if not isinstance(order_by, list):
                order_by = [order_by]
            order_by = ",".join(order_by)
            order_by = f"ORDER BY {order_by}"

        return f"""
            SELECT
                *
            FROM
                {source}
            WHERE
                {where}
            {order_by};
            """

    def get(self, **kwargs):
        """
        Execute a query against this `Query`'s source.

        Supported optional keyword arguments:

        :engine: A `sqlalchemy.engine.Engine` representing a connection to the database.

        Additional keyword arguments are forwarded to prepare_sql.

        :returns: A `pandas.DataFrame` of trips from the given provider, crossing this query's time range.
        """
        engine = kwargs.pop("engine", self.engine or ENGINE)

        sql = self.prepare_sql(**kwargs)

        if self.debug:
            print("Sending query:")
            print(sql)

        data = pandas.read_sql(sql, engine, params={"start": self.start, "end": self.end}, index_col=None)

        if self.debug:
            print(f"Got {len(data)} results")

        return data


class Availability(TimeQuery):
    """
    Represents a query of the availability windows for a particular provider.
    """

    DEFAULT_TABLE = "availability_windows"

    def __init__(self, start, end, **kwargs):
        """
        Initialize a new `Availability` query with the given parameters.

        Required positional arguments:

        :start: A python datetime, ISO8601 datetime string, or Unix timestamp for the beginning of the interval.

        :end: A python datetime, ISO8601 datetime string, or Unix timestamp for the end of the interval.

        Supported optional keyword arguments:

        :start_types: event_type or list of event_type to restrict the `start_event_type` (e.g. `available`).

        :end_types: event_type or list of event_type to restrict the `end_event_type` (e.g. `available`).

        See `TimeQuery` for additional optional keyword arguments.
        """
        self.start_types = kwargs.get("start_types")
        self.end_types = kwargs.get("end_types")

        kwargs["source"] = kwargs.get("source", self.DEFAULT_TABLE)

        super().__init__(start, end, **kwargs)

    def get(self, **kwargs):
        """
        Execute a query against the availability view.

        Supported optional keyword arguments:

        :start_types: event_type or list of event_type to restrict the start_event_type (e.g. `available`).

        :end_types: event_type or list of event_type to restrict the end_event_type (e.g. `available`).

        See `TimeQuery` for additional optional keyword arguments.

        :returns: A `pandas.DataFrame` of events from the given provider, crossing this query's time range.
        """
        predicates = []

        if "predicates" in kwargs:
            predicates = kwargs.get("predicates", [])
            predicates = [predicates] if not isinstance(predicates, list) else predicates

        ets = "'::event_types,'"
        start_types = kwargs.get("start_types", self.start_types)
        end_types = kwargs.get("end_types", self.end_types)

        if start_types:
            if not isinstance(start_types, list):
                start_types = [start_types]
            predicates.append(f"start_event_type IN ('{ets.join(start_types)}'::event_types)")

        if end_types:
            if not isinstance(end_types, list):
                end_types = [end_types]
            predicates.append(f"end_event_type IN ('{ets.join(end_types)}'::event_types)")

        kwargs["predicates"] = predicates

        return super().get(**kwargs)


class Trips(TimeQuery):
    """
    Represents a query of the trips table for a particular provider.
    """

    DEFAULT_TABLE = "trips"

    def __init__(self, start, end, **kwargs):
        """
        Initialize a new `Trips` query with the given parameters.

        Required positional arguments:

        :start: A python datetime, ISO8601 datetime string, or Unix timestamp for the beginning of the interval.

        :end: A python datetime, ISO8601 datetime string, or Unix timestamp for the end of the interval.

        Supported optional keyword arguments:

        See `TimeQuery` for additional optional keyword arguments.
        """
        kwargs["source"] = kwargs.get("source", self.DEFAULT_TABLE)

        super().__init__(start, end, **kwargs)

    def get(self, **kwargs):
        """
        Execute a query against trip records.

        Supported optional keyword arguments:

        See `TimeQuery` for additional optional keyword arguments.

        :returns: A `pandas.DataFrame` of trips from the given provider, crossing this query's time range.
        """
        return super().get(**kwargs)
