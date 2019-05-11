"""
Validate MDS provider data against the published JSON schemas.
"""

import json
from pathlib import Path
import re

from mds.providers import Provider
from mds.schemas import STATUS_CHANGES, TRIPS, DataValidator


EXCEPTIONS = [
    "is not a multiple of 1.0",
    "Payload error in links.prev",
    "Payload error in links.next",
    "Payload error in links.first",
    "Payload error in links.last",
    ".associated_trips: None is not of type 'array'",
    ".parking_verification_url: None is not of type 'string'"
]

ITEM_ERROR_REGEX = re.compile("Item error in (\w+)\[(\d+)\]")

UNEXPECTED_PROP_REGEX = re.compile("\('(\w+)' was unexpected\)")


def get_validator(record_type, ref):
    """
    Create a mds.schemas.DataValidator instance.
    """
    if record_type == STATUS_CHANGES:
        return DataValidator.status_changes(ref=ref)
    elif record_type == TRIPS:
        return DataValidator.trips(ref=ref)
    else:
        raise ValueError(f"Invalid record_type: {record_type}")


def filter(record_type, sources, **kwargs):
    """
    Keep only valid records from each source.
    """

    if not all([isinstance(d, dict) and "data" in d for d in sources]):
        raise TypeError("Sources appears to be the wrong data type. Expected a list of payload dicts.")

    def _failure(error):
        """
        Determine if the error is a real schema validation error that should cause a validation failure.

        Attempt to recover from certain types of errors and return additional context in a tuple.
        """
        description = error.describe()
        # check for exceptions
        if any([ex in description for ex in EXCEPTIONS]):
            return False, description
        # check for and remove unexpected data, returning the removed data
        unexpected_prop = UNEXPECTED_PROP_REGEX.search(description)
        if unexpected_prop:
            prop = unexpected_prop.group(1)
            data = { prop: error.instance[prop] }
            del error.instance[prop]
            return False, data
        # check for invalid data item, return index
        item_error = ITEM_ERROR_REGEX.search(description)
        if item_error:
            rt, index = item_error.group(1), int(item_error.group(2))
            if rt == record_type:
                return False, index
        # no exceptions met => failure
        return True, description

    seen = 0
    passed = 0
    valid = []
    validator = kwargs.get("validator", get_validator(record_type, kwargs["ref"]))

    print(f"Validating {record_type} @ {validator.ref}")

    for source in sources:
        data = source.get("data", {}).get(record_type, [])
        invalid = False
        invalid_index = set()
        seen += len(data)

        for error in validator.validate(source):
            # check if this error is a failure condition
            failure, ctx = _failure(error)
            invalid = invalid or failure
            # this was an invalid item error, mark it for removal
            if not failure and isinstance(ctx, int):
                invalid_index.add(ctx)
            elif failure:
                print(ctx)

        if not invalid:
            # filter invalid items if needed
            if len(invalid_index) > 0:
                data = [d for d in data if data.index(d) not in invalid_index]
                source["data"][record_type] = data
            # update the counters
            passed += len(data)
            valid.append(source)

    print(f"Validated {seen} records ({passed} passed)")

    return [source for source in valid if len(source["data"][record_type]) > 0]
