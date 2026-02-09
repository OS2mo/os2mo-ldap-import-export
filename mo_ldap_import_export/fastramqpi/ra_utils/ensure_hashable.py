# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from collections.abc import Hashable
from decimal import Decimal
from typing import Any

from frozendict import frozendict

from .dict_map import dict_map


def is_hashable(value: Any) -> bool:
    """Check if input is hashable by attempting to hashing it.

    Args:
        value: The value to be checked for hashability.

    Returns:
        Whether the value is hashable or not.
    """
    try:
        hash(value)
    except TypeError:
        return False
    return True


def is_probably_hashable(value: Any) -> bool:
    """Check if input is probably hashable without hashing it.

    *Note: To get a definite answer use `is_hashable` instead.*

    Args:
        value: The value to be checked for probable hashability.

    Returns:
        Whether the value is probably hashable or not.
    """
    return isinstance(value, Hashable)


def ensure_hashable(value: Any) -> Any:
    """Convert input into hashable equivalents if required.

    Args:
        value: The value to make hashable.

    Raises:
        ValueError: If frozendict is not installed and a dict is provided.
        TypeError: If non-hashable and non-convertable types are provided.

    Returns:
        Hashable equivalent of value, i.e. frozenset if value is a set.
    """
    if isinstance(value, dict):
        value = frozendict(
            dict_map(
                value,
                key_func=ensure_hashable,
                value_func=ensure_hashable,
            )
        )
    elif isinstance(value, set):
        value = frozenset(map(ensure_hashable, value))
    elif isinstance(value, list):
        value = tuple(map(ensure_hashable, value))
    elif isinstance(value, Decimal) and value.is_snan():
        return Decimal("nan")
    elif isinstance(value, slice):
        # Builtin and not an acceptable base type, so cannot be extended
        # I.e. it is impossible to make a 'frozenslice'.
        raise TypeError("slice cannot be made hashable")
    if not is_hashable(value):
        raise TypeError(repr(value) + " is not hashable, please report this!")
    return value
