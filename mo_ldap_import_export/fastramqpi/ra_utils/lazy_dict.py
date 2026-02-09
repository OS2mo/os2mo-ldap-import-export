# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from collections.abc import Mapping
from inspect import signature
from typing import Any
from typing import Callable
from typing import Iterator
from typing import Optional


class LazyEval:
    """Lazily evaluated dict member, used in tandem with `LazyDict`.

    For details and usage see `LazyDict`.
    """

    def __init__(self, cally: Callable, cache: bool = True) -> None:
        """Initializer.

        *Note: If the callable needs arguments, these should be provided before
               calling this initializer using `functools.partial` or similar.
               Alternatively arguments can be provided and fetched from the `LazyDict`
               itself, as this is provided during the `__call__` method.*

        Args:
            cally: The callable to execute for lazy evaluation.
            cache: Whether to cache the result in the `LazyDict` after execution.

        Returns:
            The `LazyEval` to be loaded into a `LazyDict`.
        """

        self.cally = cally
        self.cache = cache

    def do_cache(self) -> bool:
        """Check the caching strategy used.

        Returns:
            Whether the return-value of the `__call__` method should be cached.
        """
        return self.cache

    def __call__(self, key: Any, dictionary: "LazyDict") -> Any:
        """Evaluate the callable.

        Args:
            key: The key to which we are assigning this value.
            dictionary: The dictionary to which we are assigning this value.

        Returns:
            Whatever is returned by the callable `cally`.
        """
        return self.cally(key, dictionary)


def LazyEvalDerived(cally: Callable, cache: bool = True) -> LazyEval:
    """Create a 'derived' `LazyEval`.

    Derived means that the cally callable will receive arguments from the dictionary
    derived by the parameters it takes.

    Example:
        ```Python
        lazy_dict = LazyDict({"base_value": 5})
        lazy_dict["derived_val"] = LazyEvalDerived(
            lambda base_value: base_value
        )
        assert lazy_dict["derived_val"] == 5
        ```

    Args:
        cally: Callable taking any number of arguments.
               Argument names must match other `LazyDict` entries.
        cache: Same meaning as for `LazyEval`.

    Returns:
        A `LazyEval` set to evaluate a wrapper function that reads out values
        from the `LazyDict` according to the parameters taken by `cally`, before
        calling `cally` with these values.
    """
    cally_signature = signature(cally)

    def inner(key: Any, dictionary: "LazyDict") -> Any:
        return cally(
            **{varname: dictionary[varname] for varname in cally_signature.parameters}
        )

    return LazyEval(inner, cache)


def LazyEvalBare(cally: Callable, cache: bool = True) -> LazyEval:
    """Create a 'bare' `LazyEval`.

    Bare means that the cally callable will not receive any arguments.

    Example:
        ```Python
        lazy_dict = LazyDict({"base_value": 5})
        lazy_dict["bare_val"] = LazyEvalBare(
            lambda: 3.14
        )
        assert lazy_dict["bare_val"] == 3.14
        ```

    Args:
        cally: Callable taking no arguments.
        cache: Same meaning as for `LazyEval`.

    Returns:
        A `LazyEval` set to evaluate a wrapper function that calls `cally`
        without arguments.
    """

    def inner(key: Any, dictionary: "LazyDict") -> Any:
        """Throw away the arguments, and call the callable."""
        return cally()

    return LazyEval(inner, cache)


class LazyDict(Mapping):
    """Dictionary supporting lazy evaluation of some keys.

    Example:
        ```Python
        def expensive_func(n: int = 5):
            time.sleep(n)
            return n

        # Initialization finishes without sleeping
        d = LazyDicy({'a': LazyEval(expensive_func), 'b': 2})
        print(d['b'])  # --> Prints 2 immediately
        print(d['a'])  # --> Prints 5 after 5 seconds
        print(d['a'])  # --> Prints 5 immediately (cached)
        ```
    """

    def __init__(self, *args: Any, **kwargs: Optional[Any]) -> None:
        """Initialize the internal dictionary with `*args` and `**kwargs` directly.

        Args:
            args: Arguments for internal dictionary.
            kwargs: Keyword arguments for internal dictionary.

        Returns:
            dictionary ready to be used as an ordinary `dict`.
        """
        self._raw_dict = dict(*args, **kwargs)

    def __getitem__(self, key: Any) -> Any:
        """Implementation of evaluation of self[key].

        Fetches a value from the underlying dictionary, if the value is a `LazyEval`
        it is evaluated using `_handle_lazy` method, otherwise it is returned as-is.

        Args:
            key: The key to lookup

        Returns:
            If the value stored at the `key` location is, a `LazyEval`:

            * it is evaluated and its value is returned (potentially caching it),
            * otherwise the value from the internal dictionary is returned.
        """
        value = self._raw_dict.__getitem__(key)
        # Check if we got back a LazyEval item
        if isinstance(value, LazyEval):
            return self._handle_lazy(key, value)
        return value

    def __setitem__(self, key: Any, value: Any) -> None:
        self._raw_dict.__setitem__(key, value)

    def _handle_lazy(self, key: Any, lazy_eval: LazyEval) -> Any:
        """Evaluate the `LazyEval` and cache result if configured to do so."""
        value = lazy_eval(key, self)
        if lazy_eval.do_cache():
            self._raw_dict[key] = value
        return value

    def __str__(self) -> str:
        return str(self._raw_dict)

    def __repr__(self) -> str:
        return repr(self._raw_dict)

    def __iter__(self) -> Iterator:
        return iter(self._raw_dict)

    def __len__(self) -> int:
        return len(self._raw_dict)
