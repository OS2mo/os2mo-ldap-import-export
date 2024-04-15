# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
import copy
import re
from datetime import datetime
from functools import partial
from typing import Any

import structlog
from ldap3.utils.dn import parse_dn
from ldap3.utils.dn import safe_dn
from ldap3.utils.dn import to_dn
from ramodels.mo._shared import MOBase
from ramodels.mo.details.address import Address
from ramodels.mo.details.engagement import Engagement
from ramodels.mo.details.it_system import ITUser
from ramodels.mo.employee import Employee

from .customer_specific import JobTitleFromADToMO

logger = structlog.stdlib.get_logger()


def import_class(name: str) -> type[MOBase]:
    import_map: dict[str, type[MOBase]] = {
        "Custom.JobTitleFromADToMO": JobTitleFromADToMO,
        "ramodels.mo.details.address.Address": Address,
        "ramodels.mo.details.engagement.Engagement": Engagement,
        "ramodels.mo.details.it_system.ITUser": ITUser,
        "ramodels.mo.employee.Employee": Employee,
    }
    clazz = import_map.get(name)
    if clazz is None:
        raise NotImplementedError("Unknown argument to import_class")
    return clazz


# https://stackoverflow.com/questions/3405715/elegant-way-to-remove-fields-from-nested-dictionaries
def _delete_keys_from_dict(dict_del, lst_keys):
    for field in list(dict_del.keys()):
        if field in lst_keys:
            del dict_del[field]
        elif isinstance(dict_del[field], dict):
            _delete_keys_from_dict(dict_del[field], lst_keys)
    return dict_del


def delete_keys_from_dict(dict_del, lst_keys):
    """
    Delete the keys present in lst_keys from the dictionary.
    Loops recursively over nested dictionaries.
    """
    return _delete_keys_from_dict(copy.deepcopy(dict_del), lst_keys)


def mo_datestring_to_utc(datestring: str | None) -> datetime | None:
    """
    Returns datetime object at UTC+0

    Notes
    ------
    Mo datestrings are formatted like this: "2023-02-27T00:00:00+01:00"
    This function essentially removes the "+01:00" part, which gives a UTC+0 timestamp.
    """
    if datestring is None:
        return None
    return datetime.fromisoformat(datestring).replace(tzinfo=None)


def mo_object_is_valid(mo_object) -> bool:
    now = datetime.utcnow()

    if mo_object.validity.to_date is None:
        return True
    if mo_object.validity.to_date.replace(tzinfo=None) > now:
        return True
    return False


def datetime_to_ldap_timestamp(dt: datetime) -> str:
    return "".join(
        [
            dt.strftime("%Y%m%d%H%M%S"),
            ".",
            str(int(dt.microsecond / 1000)),
            (dt.strftime("%z") or "-0000"),
        ]
    )


def combine_dn_strings(dn_strings: list[str]) -> str:
    """
    Combine LDAP DN strings, skipping if a string is empty

    Examples
    ---------------
    >>> combine_dn_strings(["CN=Nick","","DC=bar"])
    >>> "CN=Nick,DC=bar"
    """
    dn: str = safe_dn(",".join(filter(None, dn_strings)))
    return dn


def remove_vowels(string: str) -> str:
    return re.sub("[aeiouAEIOU]", "", string)


def extract_part_from_dn(dn: str, index_string: str) -> str:
    """
    Extract a part from an LDAP DN string

    Examples
    -------------
    >>> extract_part_from_dn("CN=Tobias,OU=mucki,OU=bar,DC=k","OU")
    >>> "OU=mucki,OU=bar"
    """
    dn_parts = to_dn(dn)
    parts = []
    for dn_part in dn_parts:
        dn_decomposed = parse_dn(dn_part)[0]
        if dn_decomposed[0].lower() == index_string.lower():
            parts.append(dn_part)

    if not parts:
        return ""
    partial_dn: str = safe_dn(",".join(parts))
    return partial_dn


def remove_part_from_dn(dn: str, index_string: str) -> str:
    """
    Remove a part from an LDAP DN string

    Examples
    -------------
    >>> remove_part_from_dn("CN=Tobias,OU=mucki,OU=bar,DC=k","OU")
    >>> "CN=Tobias,DC=k"
    """
    dn_parts = to_dn(dn)
    parts = []
    for dn_part in dn_parts:
        dn_decomposed = parse_dn(dn_part)[0]
        if dn_decomposed[0].lower() != index_string.lower():
            parts.append(dn_part)

    if not parts:
        return ""
    partial_dn: str = safe_dn(",".join(parts))
    return partial_dn


extract_ou_from_dn = partial(extract_part_from_dn, index_string="OU")
extract_cn_from_dn = partial(extract_part_from_dn, index_string="CN")
remove_cn_from_dn = partial(remove_part_from_dn, index_string="CN")


def exchange_ou_in_dn(dn: str, new_ou: str) -> str:
    """
    Exchange the OU in a dn with another one

    Examples
    ------------
    >>> dn = "CN=Johnny,OU=foo,DC=Magenta"
    >>> new_ou = "OU=bar"
    >>> exchange_ou_in_dn(dn, new_ou)
    >>> 'CN=Johnny,OU=bar,DC=Magenta'
    """

    dn_parts = to_dn(dn)
    new_dn_parts = []
    new_ou_added = False

    for dn_part in dn_parts:
        dn_part_decomposed = parse_dn(dn_part)[0]

        if dn_part_decomposed[0].lower() == "ou":
            if not new_ou_added:
                new_dn_parts.append(new_ou)
                new_ou_added = True
        else:
            new_dn_parts.append(dn_part)

    return combine_dn_strings(new_dn_parts)


def is_list(x: Any | list[Any]) -> bool:
    """Decide whether the provided argument is a list.

    Args:
        x: A potential list.

    Returns:
        Whether the provided argument is a list or not.
    """
    return isinstance(x, list)


def is_exception(x: Any) -> bool:
    """Decide whether the provided argument is an exception.

    Args:
        x: A potential exception.

    Returns:
        Whether the provided argument is an exception or not.
    """
    return isinstance(x, Exception)


def ensure_list(x: Any | list[Any]) -> list[Any]:
    """Wrap the argument in a list if not a list.

    Args:
        x: A potential list.

    Returns:
        The provided argument unmodified, if a list.
        The provided argument wrapped in a list, if not a list.
    """
    if is_list(x):
        return x
    return [x]


# TODO: Refactor this to use structured object
def get_delete_flag(mo_object: dict[str, Any]) -> bool:
    """Determines if an object should be deleted based on the validity to-date.

    Args:
        mo_object: The object to test.

    Returns:
        Whether the object should be deleted or not.
    """
    now = datetime.utcnow()
    validity_to = mo_datestring_to_utc(mo_object["validity"]["to"])
    if validity_to and validity_to <= now:
        logger.info(
            "Returning delete=True because to_date <= current_date",
            to_date=validity_to,
            current_date=now,
        )
        return True
    return False
