# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from functools import partial
from functools import wraps
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

from jinja2 import Template


def requires_jinja(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Optional[Any]) -> Any:
        return func(*args, **kwargs)

    return wrapper


InnerFilterType = Callable[[List[str], List[Any]], bool]
FilterType = Callable[[List[Any]], bool]


def string_to_bool(v: str) -> bool:
    """Convert a string to a boolean.

    Returns `True` if lowercase version of the string is one of:
    `"yes"`, `"true"`, `"1"` or `"1.0"`

    *Note: The values for truth were chosen semi-arbitrarily.*

    Args:
        v: The string value to be converted to a boolean.

    Returns:
        The boolean interpretation of the string.
    """
    return v.lower() in ("yes", "true", "1", "1.0")


@requires_jinja
def jinja_filter(
    template: Template, tuple_keys: List[str], tuple_values: List[Any]
) -> bool:
    """Filter function to evaluate the filter on the provided argument list.

    *Note: Utilizes `tuple_keys` as keys to map the incoming list of arguments to
           jinja2 key-value context variables.*

    *Note: Utilizes `string_to_bool` to convert the output of the template into a
           boolean value.*

    Args:
        template: The `jinja2` template to put our context into.
        tuple_keys: List of keys to put into the `jinja2` template context.
        tuple_values: List of values to put into the `jinja2` template context.

    Returns:
        Whether the filter passed or not.
    """
    context: Iterator[Tuple[str, Any]] = zip(tuple_keys, tuple_values)
    context_dict: Dict[str, Any] = dict(context)
    result: str = template.render(**context_dict)
    return string_to_bool(result)


@requires_jinja
def create_filter(
    jinja_string: str,
    tuple_keys: List[str],
) -> FilterType:
    """Convert a `jinja2` filter strings into a filter function.

    Args:
        jinja_string: The filter string to be converted into a function.
        tuple_keys: List of keys to put into the `jinja2` template context.

    Returns:
        The generated filter function, to be called with `tuple_values`.
    """
    filter_function: FilterType = partial(
        jinja_filter, Template(jinja_string), tuple_keys
    )
    return filter_function


@requires_jinja
def create_filters(
    jinja_strings: List[str],
    tuple_keys: List[str],
) -> List[FilterType]:
    """Convert a list of `jinja2` filter strings into filter functions.

    For more details see `create_filter`.

    Args:
        jinja_strings: A list of filter strings to be converted into functions.
        tuple_keys: List of keys to put into the `jinja2` template contexts.

    Returns:
        A list of generated filter function, to be called with `tuple_values`.
    """

    filter_functions: Iterator[FilterType] = map(
        partial(create_filter, tuple_keys=tuple_keys), jinja_strings
    )
    return list(filter_functions)
