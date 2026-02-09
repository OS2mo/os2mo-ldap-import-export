# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Dict
from typing import List
from typing import TypeVar

from .ensure_hashable import ensure_hashable

DictKeyType = TypeVar("DictKeyType")
DictValueType = TypeVar("DictValueType")


def transpose_dict(
    dicty: Dict[DictKeyType, DictValueType],
) -> Dict[DictValueType, List[DictKeyType]]:
    """Transpose a dictionary, such that keys become values and values become keys.

    *Note: Keys actually become a list of values, rather than plain values, as value
           uniqueness is not guaranteed, and thus multiple keys may have the same
           value.*

    Example:
        ```Python
        test_dict = {'test_key1': 'test_value1'}
        tdict = transpose_dict(test_dict)
        assert tdict == {"test_value1": ["test_key1"]}
        ```

    Example:
        ```Python
        test_dict = {
            "test_key1": "test_value1",
            "test_key2": "test_value2",
            "test_key3": "test_value1",
        }
        tdict = transpose_dict(test_dict)
        assert tdict == {
            "test_value1": ["test_key1", "test_key3"],
            "test_value2": ["test_key2"]
        }
        ```

    Args:
        dicty: Dictionary to be transposed.

    Returns:
        Transposed dictionary.

        *Note: Some values may be converted to hashable equivalentes using
               `ensure_hashable` to ensure they can be used as keys.*
    """

    # Reverse the dict
    reversed_dict: Dict[DictValueType, List[DictKeyType]] = dict()
    for key, value in dicty.items():
        # Ensure all new-key (value) is hashable
        new_key = ensure_hashable(value)
        reversed_dict[new_key] = reversed_dict.get(new_key, []) + [
            key,
        ]
    return reversed_dict
