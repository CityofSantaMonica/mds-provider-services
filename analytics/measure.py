from decimal import Decimal
from datetime import datetime
from math import ceil
import pandas
from sortedcontainers import SortedDict, SortedSet


class CounterInterval(pandas.Interval):
    """
    Represents an interval of time within which devices are counted.
    """

    def __init__(self, start, end):
        """
        Initialize a new `CounterInterval` instance for the given start and end times.

        The interval is closed [start, end], such that it contains both endpoints.
        """
        # sanity check
        if start > end:
            start, end = end, start

        super().__init__(left=int(start), right=int(end), closed="both")

    @property
    def start(self):
        return self.left

    @property
    def end(self):
        return self.right

    @property
    def delta(self):
        return self.length


class DeviceCounter():
    """
    Measure the number of available devices within a time period.
    """

    def __init__(self, start, end, local=False, debug=False, **kwargs):
        """
        Initialize a new `DeviceCounter` instance for the given range of time.

        Required positional arguments:

        :start: A python `datetime`, pandas `Timestamp`, or Unix timestamp for the beginning of the counting interval.

        :end: A python `datetime`, pandas `Timestamp`, or Unix timestamp for the end of the counting interval.

        Optional keyword arguments:

        :local: `False` (default) to assume Unix time; `True` to assume local time.

        :debug: `False` (default) to supress debug messages; `True` to print to stdout.
        """
        if start is None or end is None:
            raise TypeError(f"'NoneType' was unexpected for start and/or end. Expected datetime, Timestamp, or Unix timestamp")

        self.start = start
        self.end = end
        self._start = self._ts2int(start)
        self._end = self._ts2int(end)
        self.interval = CounterInterval(self._start, self._end)
        self.delta = self.interval.delta
        self.local = local
        self.debug = debug

        self._reset()

        if self.debug:
            print(f"self.interval: {self.interval}")
            print()

    def _reset(self):
        """
        Resets this counter with the initial interval.
        """
        self.counts = SortedDict({ self.interval : 0 })

        # debug info
        self.events = 0
        self.splits = 0
        self.counter = 0

    def _int2ts(self, i):
        """
        Convert :i: to a Timestamp
        """
        return pandas.Timestamp(i, unit="s")

    def _ts2int(self, ts):
        """
        Try to convert :ts: to a integer
        """
        try:
            return int(ts.timestamp())
        except:
            return int(ts)

    def _interval(self, key_index):
        """
        Get the Interval by index in the sorted key list
        """
        return self.counts.keys()[key_index]

    def _insertidx(self, start, end, default_index=0):
        """
        Get an insertion index for an interval with the given endpoints.
        """
        # the index for the closest known sub-interval to the event's timespan
        index = self.counts.bisect_right(CounterInterval(start, end or self._end)) - 1
        # using the start of the interval as the default
        return index if index >= 0 else default_index

    def count_event(self, event_start, event_end):
        """
        Increment the counter for the given interval of time.

        :event_start: A python `datetime`, pandas `Timestamp`, or Unix timestamp marking the beginning of the event interval.

        :event_end: A python `datetime`, pandas `Timestamp`, or Unix timestamp for the end of the event interval, or `None` for
        and event with an open interval.

        Performs a right-bisection on the counter intervals, assigning counts to increasingly
        finer slices based on the the event's timespan's intersection with the existing counter intervals.
        """
        event_start = self._ts2int(event_start)
        event_end = None if (event_end is None or event_end is pandas.NaT) else self._ts2int(event_end)
        to_remove = SortedSet()
        to_add = SortedDict()

        _counter = self.counter
        _splits = self.splits

        # get the next insertion index
        index = self._insertidx(start=event_start, end=event_end)

        # move the index to the right, splitting the existing intervals and incrementing counts along the way
        while index < len(self.counts) and (event_end is None or self._interval(index).start < event_end):
            interval = self._interval(index)
            count = self.counts[interval]
            start, end = interval.start, interval.end

            # the event has a closed timespan: [event_start, event_end]
            if event_end is not None:
                # event fully spans and contains the interval
                if event_start <= start and event_end >= end:
                    # increment the interval
                    to_add[CounterInterval(start, end)] = count + 1
                    self.counter += 1

                # event starts before the interval and overlaps from the left
                elif event_start <= start and event_end > start and event_end < end:
                    # subdivide and increment the affected sub-interval
                    # [start, end] -> [start, event_end]+, [event_end, end]
                    to_remove.add(interval)
                    to_add[CounterInterval(start, event_end)] = count + 1
                    to_add[CounterInterval(event_end, end)] = count
                    self.splits += 1
                    self.counter += 1

                # event starts in the interval and overlaps on the right
                elif event_start > start and event_start < end and event_end >= end:
                    # subdivide and increment the affected interval
                    # [start, end] -> [start, event_start], [event_start, end]+
                    to_remove.add(interval)
                    to_add[CounterInterval(start, event_start)] = count
                    to_add[CounterInterval(event_start, end)] = count + 1
                    self.splits += 1
                    self.counter += 1

                # event is fully within and contained by the interval
                elif event_start > start and event_end < end:
                    # subdivide and increment the affected interval
                    # [start, end] -> [start, event_start], [event_start, event_end]+, [event_end, end]
                    to_remove.add(interval)
                    to_add[CounterInterval(start, event_start)] = count
                    to_add[CounterInterval(event_start, event_end)] = count + 1
                    to_add[CounterInterval(event_end, end)] = count
                    self.splits += 2
                    self.counter += 1

            # the event has an open timespan: [event_start, )
            else:
                # event starts before the interval
                if event_start <= start:
                    # incrememnt the interval
                    to_add[CounterInterval(start, end)] = count + 1
                    self.counter += 1

                # event starts inside the interval
                elif event_start > start and event_start <= end:
                    # subdivide and increment the affected interval
                    # [start, end] -> [start, event_start], [event_start, end]+
                    to_remove.add(interval)
                    to_add[CounterInterval(start, event_start)] = count
                    to_add[CounterInterval(event_start, end)] = count + 1
                    self.splits += 1
                    self.counter += 1

            index += 1

        for r in to_remove:
            self.counts.pop(r)

        for k in to_add.keys():
            self.counts[k] = to_add[k]

        if self.debug:
            debug = {
                "start": event_start,
                "end": event_end,
                "index": index,
                "remove": len(to_remove),
                "split": int(self.splits - _splits),
                "add": len(to_add),
                "counter": int(self.counter - _counter)
            }
            print(", ".join([f"{k}: {v}" for k, v in debug.items()]))

        self.events += 1

        return self

    def count(self, data, predicate=None):
        """
        Count device availability observed in data, over this counter's interval.

        :data: A `pandas.DataFrame` of records from the availability view.

        :predicate: A function with 3 positional args: this `DeviceCounter`, an index, and corresponding row from :data:.
        This function will be called before the given row is evaluated; if `True`, the row is counted.

        :returns: This `DeviceCounter` instance.
        """
        if self.debug:
            print(f"Generating f(x) over [{self.start}, {self.end}] with {len(data)} input records")
            print()

        self._reset()

        assert(len(self.counts) == 1)
        assert(self.counts.keys()[0] == self.interval)

        scale = ceil(len(data) / 10)

        # using this counter's initial interval as a starting point,
        # subdivide based on the intersection of the interval from each event in the data
        # incrememting a counter for each sub-interval created along the way
        for index, row in data.iterrows():
            if self.debug and index % scale == 0:
                print(f"Processing {index + 1} of {len(data)}")

            if predicate is None or predicate(self, index, row):
                if self.local:
                    self.count_event(row["start_time_local"], row["end_time_local"])
                else:
                    self.count_event(row["start_time"], row["end_time"])

        if self.debug:
            print("Partitioning complete.")
            print(f"events: {self.events}, splits: {self.splits}, counter: {self.counter}")

        return self

    def partition(self):
        """
        Returns the current interval partition as a `pandas.DataFrame`.
        """
        partition = [{ "start": i.start,
                        "end": i.end,
                        "delta": i.delta,
                        "count": c,
                        "start_date": self._int2ts(i.start),
                        "end_date": self._int2ts(i.end) }
                    for i, c in self.counts.items()]

        return pandas.DataFrame.from_records(partition,
            columns=["start", "end", "delta", "count", "start_date", "end_date"])

    def delta_x(self):
        """
        :return: The ordered list of deltas for the given interval partition, or this interval's partition.
        """
        partition = self.partition()
        return partition["delta"]

    def norm(self):
        """
        Get the delta of the largest sub-interval in this interval's partition.
        """
        partition = self.partition()
        return max(self.delta_x())

    def dimension(self):
        """
        The number of sub-intervals in this interval's partition.
        """
        return len(self.partition())

    def average(self):
        """
        Estimate the average number of devices within this interval's partition.

        Use a Riemann sum to estimate, computing the area of each sub-interval in the partition:

        - height: the count of devices seen during that timeslice
        - width:  the length of the timeslice in seconds
        """
        partition = self.partition()

        if self.debug:
            print(f"Computing average across {self.dimension()} subintervals.")

        areas = partition.apply(lambda i: i["count"] * i["delta"], axis="columns")
        sigma = areas.agg("sum")

        if self.debug:
            print("sigma:", sigma)
            print("delta:", self.delta)

        # Compute the average value over this counter's interval
        return sigma / self.delta
