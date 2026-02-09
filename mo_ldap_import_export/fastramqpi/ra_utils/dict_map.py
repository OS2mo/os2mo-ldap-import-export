# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional

from more_itertools import unzip


def dict_map(
    dicty: Dict,
    key_func: Optional[Callable[[Any], Any]] = None,
    value_func: Optional[Callable[[Any], Any]] = None,
) -> Dict:
    """Map a dictionary's keys and values.

    Similar to the built-in `map` function, but for dictionaries.

    Example:
        ```Python
        pow2 = lambda x: x**2
        input_dict = {1: 1, 2: 2, 3: 3}

        output_dict = dict_map(input_dict, value_func=pow2)
        assert output_dict == {1: 1, 2: 4, 3: 9})

        output_dict = dict_map(input_dict, key_func=pow2)
        assert output_dict == {1: 1, 4: 2, 9: 3})
        ```

    Args:
        dicty:
            The dictionary to transform keys and values from.
        key_func:
            The function to apply to each key in the dictionary.

            *Note: Care must be taken for mapped keys to not destructively
                   interfere with one another, i.e. the `key_func` should be a
                   bijective function.*
        value_func:
            The function to apply to each value in the dictionary.

    Raises:
        ValueError: Raised if provided the `key_func` was not a bijective function.

    Returns:
        A dict where the functions has been applied to keys and values respectively.
    """
    # Handle base-cases, i.e. empty dict and no transformation
    if not dicty:
        return dicty
    if key_func is None and value_func is None:
        return dicty

    keys, values = unzip(dicty.items())
    if key_func:
        keys = map(key_func, keys)
    if value_func:
        values = map(value_func, values)
    result_dict = dict(zip(keys, values))

    if len(result_dict) != len(dicty):
        raise ValueError("Provided `key_func` is non-bijective")
    return result_dict


def dict_map_key(key_func: Callable[[Any], Any], dicty: Dict) -> Dict:
    """Map a dictionary's keys.

    Similar to the built-in `map` function, but for dictionary keys.

    *Note: Utilizes `dict_map` internally, but swaps argument order to be more
           in line with the built-in `map` function.*

    Example:
        ```Python
        pow2 = lambda x: x**2
        input_dict = {1: 1, 2: 2, 3: 3}

        output_dict = dict_map_key(pow2, input_dict)
        assert output_dict == {1: 1, 4: 2, 9: 3}
        ```

    Args:
        dicty:
            The dictionary to transform keys and values from.
        key_func:
            The function to apply to each key in the dictionary.

            *Note: Care must be taken for mapped keys to not destructively
                   interfere with one another, i.e. the `key_func` should be a
                   bijective function.*

    Raises:
        ValueError: Raised if provided the `key_func` was not a bijective function.

    Returns:
        A dict where the `key_func` has been applied to every key.
    """
    return dict_map(dicty, key_func=key_func)


def dict_map_value(value_func: Callable[[Any], Any], dicty: Dict) -> Dict:
    """Map a dictionary's values.

    Similar to the built-in `map` function, but for dictionary values.

    *Note: Utilizes `dict_map` internally, but swaps argument order to be more
           in line with the built-in `map` function.*

    Example:
        ```Python
        pow2 = lambda x: x**2
        input_dict = {1: 1, 2: 2, 3: 3}

        output_dict = dict_map_value(pow2, input_dict)
        assert output_dict == {1: 1, 2: 4, 3: 9}
        ```

    Args:
        dicty:
            The dictionary to transform keys and values from.
        value_func:
            The function to apply to each value in the dictionary.

    Returns:
        A dict where the `value_func` has been applied to every value.
    """
    return dict_map(dicty, value_func=value_func)
