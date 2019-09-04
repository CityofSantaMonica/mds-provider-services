"""
Validate MDS provider data against the published JSON schemas.
"""

import argparse
import datetime
import json
import os
import re

import mds

import common


_EXCEPTIONS = [
    "is not a multiple of 1.0",
    "Payload error in links.prev",
    "Payload error in links.next",
    "Payload error in links.first",
    "Payload error in links.last",
    ".associated_trips: None is not of type 'array'",
    ".parking_verification_url: None is not of type 'string'",
    "valid under each of {'required': ['associated_trip']}"
]

_ITEM_ERROR = re.compile("Item error in (\w+)\[(\d+)\]")

_UNEXPECTED_PROP = re.compile("\('(\w+)' was unexpected\)")


def _validator(record_type, ref):
    """
    Create a DataValidator instance.
    """
    if record_type == mds.STATUS_CHANGES:
        return mds.DataValidator.status_changes(ref=ref)
    elif record_type == mds.TRIPS:
        return mds.DataValidator.trips(ref=ref)
    else:
        raise ValueError(f"Invalid record_type: {record_type}")


def _failure(record_type, error):
    """
    Determine if the error is a real schema validation error that should cause a validation failure.

    Attempt to recover from certain types of errors and return additional context in a tuple.
    """
    # describing an error returns a list of messages, so join with a linesep
    description = os.linesep.join(error.describe())

    # check for exceptions
    if any([ex in description for ex in _EXCEPTIONS]):
        return False, description

    # check for and remove unexpected data, returning the removed data
    unexpected_prop = _UNEXPECTED_PROP.search(description)
    if unexpected_prop:
        prop = unexpected_prop.group(1)
        data = { prop: error.instance[prop] }
        del error.instance[prop]
        return False, data

    # check for invalid data item, return index
    item_error = _ITEM_ERROR.search(description)
    if item_error:
        rt, index = item_error.group(1), int(item_error.group(2))
        if rt == record_type:
            return False, index

    # no exceptions met => failure
    return True, description


def _validate_provider(provider, **kwargs):
    """
    Validate the feeds for a provider.
    """
    config = common.get_config(provider, kwargs.get("config"))

    # assert the version parameter
    version = mds.Version(config.pop("version", kwargs.get("version")))
    if version.unsupported:
        raise mds.UnsupportedVersionError(version)
    else:
        kwargs["version"] = version

    # request a reasonably recent span of time (the hour beginning 25 hours before the current time):
    end = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    start = end - datetime.timedelta(seconds=3600)

    kwargs["start_time"] = start
    kwargs["end_time"] = end
    kwargs["no_paging"] = False
    kwargs["rate_limit"] = 0
    kwargs["client"] = mds.Client(provider, version=version, **config)

    output = kwargs.pop("output", None)

    results = []

    # check each of the feeds
    for record_type in [mds.STATUS_CHANGES, mds.TRIPS]:
        datasource = common.get_data(record_type, **kwargs)

        # output to files if needed
        if output:
            common.write_data(record_type, datasource, output)

        # validate this record_type/version combo
        result = _validate(record_type, datasource, version)
        results.append(result)

    return results


def _validate_file(path, **kwargs):
    """
    Validate data from the filesystem.
    """
    kwargs["source"] = path
    results = []

    for record_type in [mds.STATUS_CHANGES, mds.TRIPS]:
        datasource = common.get_data(record_type, **kwargs)

        if len(datasource) > 0:
            result = _validate(record_type, datasource)
            results.append(result)
        else:
            print("No records")

    return results


def _validate(record_type, datasource, version=None):
    """
    Validate a list of MDS payloads.
    """
    versions = set([d["version"] for d in datasource])

    if len(versions) > 1:
        return (record_type, mds.Version(versions.pop()), False)

    version = mds.Version(version or versions.pop())

    filtered = keep_valid(record_type, datasource, version)
    result = len(datasource) == len(filtered)

    if result:
        for ds, fs in zip(datasource, filtered):
            if len(ds["data"][record_type]) != len(fs["data"][record_type]):
                result = False
                break

    return (record_type, version, result)


def keep_valid(record_type, sources, version, **kwargs):
    """
    Keep only valid records from each source.
    """
    if not all([isinstance(d, dict) and "data" in d for d in sources]):
        raise TypeError("Sources appears to be the wrong data type. Expected a list of payload dicts.")

    source_versions = [mds.Version(d["version"]) for d in sources]

    if any([version != v for v in source_versions]):
        raise mds.versions.UnexpectedVersionError(source_versions[0], version)

    seen = 0
    passed = 0
    valid = []
    validator = kwargs.get("validator", _validator(record_type, version))

    for source in sources:
        records = source.get("data", {}).get(record_type, [])
        invalid = False
        invalid_index = set()
        seen += len(records)

        for error in validator.validate(source):
            failure, ctx = _failure(record_type, error)
            invalid = invalid or failure

            # this was an invalid item error, mark it for removal
            if not failure and isinstance(ctx, int):
                invalid_index.add(ctx)
            elif failure:
                print(ctx)

        if not invalid:
            # filter invalid items if needed
            if len(invalid_index) > 0:
                valid_records = [d for d in records if records.index(d) not in invalid_index]
            else:
                valid_records = list(records)

            passed += len(valid_records)
            if len(valid_records) > 0:
                # create a copy to preserve the original payload
                valid_payload =  { **source, "data": { record_type: valid_records } }
                valid.append(valid_payload)

    print(f"Validated {seen} records ({passed} passed)")

    return valid


def setup_cli():
    """
    Create the cli argument interface, and parses incoming args.

    Returns a tuple:
        - the argument parser
        - the parsed args
    """
    parser = argparse.ArgumentParser(description="Validate MDS data feeds.")

    parser.add_argument(
        "source",
        type=str,
        nargs="+",
        help="The name or identifier of a provider to validate; or\
        One or more paths to (directories containing) MDS Provider JSON file(s) to validate."
    )

    parser.add_argument(
        "--auth_type",
        type=str,
        default="Bearer",
        help="The type used for the Authorization header for requests to the provider\
        (e.g. Basic, Bearer)."
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Path to a provider configuration file to use."
    )
    parser.add_argument(
        "-H",
        "--header",
        dest="headers",
        action="append",
        type=lambda kv: (kv.split(":", 1)[0].strip(), kv.split(":", 1)[1].strip()),
        default=[],
        help="One or more 'Header: value' combinations, sent with each request."
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Write results to json files in this directory."
    )
    parser.add_argument(
        "--version",
        type=lambda v: mds.Version(v),
        default=common.default_version,
        help=f"The release version at which to reference MDS, e.g. {common.default_version}"
    )

    return parser, parser.parse_args()


if __name__ == "__main__":
    now = datetime.datetime.utcnow()

    arg_parser, args = setup_cli()
    kwargs = vars(args)

    print(f"Starting validation run: {now.isoformat()}")

    sources = kwargs.pop("source")
    for source in sources:
        results = []

        try:
            results = _validate_provider(source, **kwargs)
        except:
            results = _validate_file(source, **kwargs)

        print(f"Validation results for '{source}'")
        for (record_type, version, result) in results:
            valid = "valid \u2714" if result else "invalid \U0001D5EB"
            print(f"  - {record_type} @ {version}: {valid}")

    print(f"Finished validation ({common.count_seconds(now)}s)")
