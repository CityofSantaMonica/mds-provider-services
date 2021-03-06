"""
Validate MDS provider data against the published JSON schemas.
"""

import datetime
import os
import pathlib
import re

import mds

import common


_FILTER_EXCEPTIONS = [
    re.compile("Item error")
]

_KEEP_EXCEPTIONS = [
    re.compile("Item error in status_changes\[\d+\]\s+\{(?!.*'associated_trip').+\} is not valid under any of the given schemas"),
    re.compile("valid under each of \{'required': \['associated_trip'\]\}")
]

_UNEXPECTED_PROP = re.compile("\('(\w+)' was unexpected\)")


def _validator(record_type, ref):
    """
    Create a DataValidator instance.
    """
    if record_type == mds.EVENTS:
        return mds.DataValidator.events(ref)
    elif record_type == mds.STATUS_CHANGES:
        return mds.DataValidator.status_changes(ref)
    elif record_type == mds.TRIPS:
        return mds.DataValidator.trips(ref)
    elif record_type == mds.VEHICLES:
        return mds.DataValidator.vehicles(ref)
    else:
        raise ValueError(f"Invalid record_type: {record_type}")


def _failure(error):
    """
    Determine if the error is a real schema validation error that should cause a validation failure.
    """
    # describing an error returns a list of messages, so join with a linesep
    description = os.linesep.join(error.describe())

    # check for and remove unexpected data
    unexpected_prop = _UNEXPECTED_PROP.search(description)
    if unexpected_prop:
        prop = unexpected_prop.group(1)
        del error.instance[prop]
        return False, None

    # check for exceptions for records that should be kept
    if any([ex.search(description) for ex in _KEEP_EXCEPTIONS]):
        return False, None

    # check for exceptions for records that should be removed
    if any([ex.search(description) for ex in _FILTER_EXCEPTIONS]):
        idx = list(filter(lambda i: isinstance(i, int), error.path))[0]
        return False, idx

    # no exceptions met => failure
    return True, None


def _validate_provider(provider, **kwargs):
    """
    Validate the feeds for a provider.
    """
    # compute a time query range; one or both sides may not be relevant for all feeds.
    if "start_time" not in kwargs and "end_time" not in kwargs:
        # default to the hour beginning 25 hours before the current time
        end = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        start = end - datetime.timedelta(seconds=3600)
    elif "start_time" not in kwargs or "end_time" not in kwargs:
        # one side of range provided, compute the other side for a total range of an hour
        start, end = common.parse_time_range(duration=3600, **kwargs)
    else:
        # both sides of range provided
        start, end = common.parse_time_range(**kwargs)

    kwargs["start_time"] = start
    kwargs["end_time"] = end

    config = common.get_config(provider, kwargs.get("config"))

    # assert the version parameter
    version = mds.Version(config.pop("version", kwargs.get("version")))
    version.raise_if_unsupported()

    kwargs["version"] = version
    kwargs["no_paging"] = False
    kwargs["rate_limit"] = 0
    kwargs["client"] = mds.Client(provider, version=version, **config)

    return _validate(**kwargs)


def _validate_file(path, **kwargs):
    """
    Validate data from the filesystem.
    """
    kwargs["source"] = path

    return _validate(**kwargs)


def _validate(**kwargs):
    """
    Check each feed type and keep valid results
    """
    results = []
    version = kwargs["version"]

    for record_type in mds.SCHEMA_TYPES:
        datasource = common.get_data(record_type, **kwargs)

        if len(datasource) > 0:
            versions = set([d["version"] for d in datasource])

            if len(versions) > 1:
                expected, unexpected = mds.Version(versions.pop()), mds.Version(versions.pop())
                error = mds.versions.UnexpectedVersionError(expected, unexpected)
                results.append((record_type, expected, datasource, [], [error], []))
                continue

            version = mds.Version(version or versions.pop())

            try:
                valid, errors, removed = validate(record_type, datasource, version)
                results.append((record_type, version, datasource, valid, errors, removed))
            except mds.versions.UnexpectedVersionError as unexpected_version:
                results.append((record_type, version, datasource, [], [unexpected_version], []))

    return results


def validate(record_type, sources, version, **kwargs):
    """
    Partition sources into a tuple of (valid, errors, failures)

        - valid: the sources with remaining valid data records
        - errors: a list of mds.schemas.DataValidationError
        - removed: the sources with invalid data records
    """
    if not all([isinstance(d, dict) and "data" in d for d in sources]):
        raise TypeError("Sources appears to be the wrong data type. Expected a list of payload dicts.")

    source_versions = [mds.Version(d["version"]) for d in sources]

    if any([version != v for v in source_versions]):
        raise mds.versions.UnexpectedVersionError(source_versions[0], version)

    valid = []
    errors = []
    removed = []
    validator = kwargs.get("validator", _validator(record_type, version))
    data_key = validator.data_key

    for source in sources:
        records = list(source.get("data", {}).get(data_key, []))
        invalid_records = []
        invalid_source = False
        invalid_idx = set()

        # schema validation
        for error in validator.validate(source):
            errors.append(error)
            failure, idx = _failure(error)
            invalid_source = invalid_source or failure

            # this was a problem with a single item, mark it for removal
            if not failure and isinstance(idx, int):
                invalid_idx.add(idx)

        # filter invalid items if the overall payload was OK
        if not invalid_source:
            if len(invalid_idx) > 0:
                valid_records = [r for r in records if records.index(r) not in invalid_idx]
                invalid_records = [r for r in records if records.index(r) in invalid_idx]
            else:
                valid_records = records

            if len(valid_records) > 0:
                # create a copy to preserve the original payload
                payload = { **source, "data": { data_key: valid_records } }
                valid.append(payload)

            if len(invalid_records) > 0:
                # create a copy to preserve the original payload
                payload = { **source, "data": { data_key: invalid_records } }
                removed.append(payload)

    return valid, errors, removed


def setup_cli():
    """
    Create the cli argument interface, and parses incoming args.

    Returns a tuple:
        - the argument parser
        - the parsed args
    """
    parser = common.setup_cli(description="Validate MDS data feeds.")

    parser.add_argument(
        "source",
        type=str,
        nargs="+",
        help="The name or identifier of a provider to validate; or\
        One or more paths to (directories containing) MDS Provider JSON file(s) to validate."
    )

    parser.add_argument(
        "--end_time",
        type=str,
        help="The end of the time query range for this request.\
        Should be either numeric Unix time or ISO-8601 datetime format."
    )

    parser.add_argument(
        "--start_time",
        type=str,
        help="The beginning of the time query range for this request.\
        Should be either numeric Unix time or ISO-8601 datetime format."
    )

    return parser, parser.parse_args()


if __name__ == "__main__":
    now = datetime.datetime.utcnow()

    arg_parser, args = setup_cli()
    kwargs = vars(args)

    print(f"Starting validation run: {now.isoformat()}")

    for source in kwargs.pop("source"):
        print()
        print(f"Validating {source} @ {args.version}")

        results = []

        try:
            if pathlib.Path(source).exists():
                results = _validate_file(source, **kwargs)
            else:
                results = _validate_provider(source, **kwargs)
        except mds.versions.UnexpectedVersionError as unexpected_version:
            print(unexpected_version)

        if len(results) == 0:
            continue

        print(f"Validation results for {source}")

        for record_type, version, original, valid, errors, invalid in results:
            data_key = mds.Schema(record_type).data_key
            seen = sum([len(o["data"][data_key]) for o in original])
            passed = sum([len(v["data"][data_key]) for v in valid])
            removed = sum([len(i["data"][data_key]) for i in invalid])
            result = len(original) == len(valid) and seen == passed
            icon = "\u2714" if result else "\U0001D5EB"

            print()
            print(f"{icon} {record_type}, version {version}")
            print(f"  {seen} records, {passed} valid, {removed} invalid")

            if len(errors) > 0:
                print(f"  Errors ({len(errors)} total)")
                for error in errors:
                    print()
                    try:
                        for line in error.describe():
                            print(f"    {line}")
                    except:
                        print(error)

            if args.output:
                print()
                print(f"Writing {record_type} to {args.output}")

                f = mds.DataFile(record_type, args.output)

                f.dump_payloads(original, file_name=f"{source}_{record_type}_original.json")
                f.dump_payloads(valid, file_name=f"{source}_{record_type}_valid.json")

                if len(invalid) > 0:
                    f.dump_payloads(invalid, file_name=f"{source}_{record_type}_invalid.json")

    print()
    print(f"Finished validation ({common.count_seconds(now)}s)")
