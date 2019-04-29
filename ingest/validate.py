"""
Validate MDS provider data against the published JSON schemas.
"""

import json
from pathlib import Path
import re

from mds import Provider, ProviderDataValidator, STATUS_CHANGES, TRIPS


exceptions = [
    "is not a multiple of 1.0",
    "Payload error in links.prev",
    "Payload error in links.next",
    "Payload error in links.first",
    "Payload error in links.last",
    ".associated_trips: None is not of type 'array'",
    ".parking_verification_url: None is not of type 'string'"
]

item_error_regex = re.compile("Item error in (\w+)\[(\d+)\]")

unexpected_prop_regex = re.compile("\('(\w+)' was unexpected\)")


def filter_valid(record_type, pages, **kwargs):
    """
    Return only the valid records from each page of data of the given type.

    Either a validator instance via the :validator: kwarg, or a :ref: kwarg to obtain a validator instance, is required.
    """
    def __failure(error):
        """
        Determine if the error is a real schema validation error that should cause a validation failure.

        Attempt to recover from certain types of errors and return additional context in a tuple.
        """
        description = error.describe()
        # check for exceptions
        if any([ex in description for ex in exceptions]):
            return False, description
        # check for and remove unexpected data, returning the removed data
        unexpected_prop = unexpected_prop_regex.search(description)
        if unexpected_prop:
            prop = unexpected_prop.group(1)
            data = { prop: error.instance[prop] }
            del error.instance[prop]
            return False, data
        # check for invalid data item, return index
        item_error = item_error_regex.search(description)
        if item_error:
            rt, index = item_error.group(1), int(item_error.group(2))
            if rt == record_type:
                return False, index
        # no exceptions met => failure
        return True, description

    seen = 0
    passed = 0
    valid = []
    validator = kwargs.get("validator", get_validator(record_type, kwargs.get("ref")))

    for page in pages:
        data = page.get("data", {}).get(record_type, [])
        invalid = False
        invalid_index = set()
        seen += len(data)

        for error in validator.validate(page):
            # check if this error is a failure condition
            failure, ctx = __failure(error)
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
                page["data"][record_type] = data
            # update the counters
            passed += len(data)
            valid.append(page)

    print(f"Validated {seen} records ({passed} passed)")
    return valid


def get_validator(record_type, ref):
    if record_type == mds.STATUS_CHANGES:
        return ProviderDataValidator.StatusChanges(ref=ref)
    elif record_type == mds.TRIPS:
        return ProviderDataValidator.Trips(ref=ref)
    else:
        raise ValueError(f"Invalid record_type: {record_type}")


def validate_data(source, record_type, ref):
    """
    Validate MDS provider data, which could be:

        - a dict of Provider to [list of data pages]
        - a list of JSON file paths

    Returns a dict of source to valid data from source.
    """
    print(f"Validating {record_type}")

    validator = get_validator(record_type, ref)
    valid = {}

    for provider in source:
        if isinstance(provider, Provider):
            print("Validating data from", provider.provider_name)
            pages = source[provider]
        elif isinstance(provider, Path):
            print("Validating data from", provider)
            pages = json.load(provider.open("r"))
        elif isinstance(provider, str):
            print("Validating data from", provider)
            pages = json.load(open(provider, "r"))
        else:
            print("Skipping", provider)
            continue

        valid[provider] = filter_valid(record_type, pages, validator=validator)

    return valid
