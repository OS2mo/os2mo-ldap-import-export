# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import warnings
from functools import wraps
from typing import Any
from typing import Callable
from typing import Optional


def deprecated(func: Callable) -> Callable:
    """Function decorator to mark a function as deprecated.

    Example:
        ```Python
        @deprecated
        def old_function():
            pass

        old_function()
        # <stdin>:1: DeprecationWarning: Call to deprecated function func.
        ```

    Args:
        func: The function to mark as deprecated.

    Returns:
        The function which emits warnings when used.
    """

    @wraps(func)
    def new_func(*args: Any, **kwargs: Optional[Any]) -> Any:
        warnings.warn(
            "Call to deprecated function {}.".format(func.__name__),
            category=DeprecationWarning,
            stacklevel=2,
        )
        return func(*args, **kwargs)

    return new_func
