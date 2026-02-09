# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from functools import wraps
from inspect import signature
from typing import Any
from typing import Callable
from typing import Tuple
from typing import TypeVar

CallableReturnType = TypeVar("CallableReturnType")


def has_self_arg(func: Callable) -> bool:
    """Return `True` if the given callable `func` needs `self` explicitly passed.

    Example:
        ```Python
        class Classy:
            def method(self):
                pass

            @classmethod
            def classmethod(cls):
                pass

            @staticmethod
            def staticmethod():
                pass

        def function():
            pass

        classy = Classy()
        # Can be passed implicitly by calling with 'classy.'
        assert has_self_arg(classy.method) is False
        assert has_self_arg(Classy.method) is True

        # These never need 'self' passed explicitly
        assert has_self_arg(classy.classmethod) is False
        assert has_self_arg(Classy.classmethod) is False
        assert has_self_arg(classy.staticmethod) is False
        assert has_self_arg(Classy.staticmethod) is False
        assert has_self_arg(function) is False
        ```

    Args:
        func (function): The function or method to check.

    Returns:
        Whether the provided function needs `self` passed explicitly.
    """
    args = list(signature(func).parameters)
    return bool(args) and args[0] in ("self")


def apply(func: Callable[..., CallableReturnType]) -> Callable[..., CallableReturnType]:
    """Function decorator to apply tuple to function.

    Example:
        ```Python
        @apply
        def dual(key, value):
            return value

        print(dual(('k', 'v')))  # --> 'v'
        ```

    Args:
        func: The function to apply arguments for.

    Returns:
        The function which has had it argument applied.
    """

    if has_self_arg(func):

        @wraps(func)
        def wrapper(self: Any, tup: Tuple) -> CallableReturnType:
            return func(self, *tup)

    else:

        @wraps(func)
        def wrapper(tup: Tuple) -> CallableReturnType:
            return func(*tup)

    return wrapper
