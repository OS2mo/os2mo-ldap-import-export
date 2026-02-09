# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from asyncio import get_event_loop
from asyncio import iscoroutinefunction
from asyncio import new_event_loop
from functools import partial
from functools import wraps
from types import TracebackType
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Optional
from typing import Type
from typing import TypeVar
from typing import Union

CoroutineReturnType = TypeVar("CoroutineReturnType")


class Syncable:
    """Helper mixin to support synchronized use of async classes.

    Must be given before the async class in inheritance hierarchy.

    *Note: Works by overridding `__getattribute__` to check for coroutine lookups
           and wrapping them in `asyncio.loop.run_until_complete` if an active
           event-loop is not detected.*

    Example:
        Basic usage:
        ```Python
        import asyncio

        class AsyncSleeper:
            async def sleep(self, time: int) -> int:
                await asyncio.sleep(time)
                return time

        class Sleeper(Syncable, AsyncSleeper):
            pass  # Syncable must come before AsyncSleeper

        async def async_call(time: int) -> int:
            return await Sleeper().sleep(time)

        def sync_call(time: int) -> int:
            return Sleeper().sleep(time)

        print(asyncio.run(async_call(1)))  # --> prints "1" after 1 second
        print(sync_call(2))                # --> prints "2" after 2 second
        ```

    Example:
        Context manager usage:
        ```Python
        import asyncio

        class AsyncContext:
            async def __aenter__(self):
                print("aenter")
                return self

            async def __aexit__(self, *err):
                print("aexit")

        class Context(Syncable, AsyncContext):
            pass  # Syncable must come before AsyncSleeper


        async def async_call() -> None:
            context = Context()
            async with context:
                print("inside")

        def sync_call() -> None:
            context = Context()
            with context:
                print("inside")

        # Both print "aenter", "inside", "aexit"
        asyncio.run(async_call())
        sync_call()
        ```
    """

    def __init__(self, *args: Any, **kwargs: Optional[Any]) -> None:
        super().__init__(*args, **kwargs)  # type: ignore
        try:
            self.__loop = get_event_loop()
        except RuntimeError:
            self.__loop = new_event_loop()

    def __enter__(self) -> None:
        """Call `__aenter__` if parent has it, `AttributeError` otherwise."""
        if hasattr(self, "__aenter__"):
            return self.__aenter__()  # type: ignore
        raise AttributeError("__enter__")

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        exc_traceback: Optional[TracebackType],
    ) -> bool:
        """Call `__aexit__` if parent has it, `AttributeError` otherwise."""
        if hasattr(self, "__aexit__"):
            return self.__aexit__(exc_type, exc_value, exc_traceback)  # type: ignore
        raise AttributeError("__exit__")

    def __run_coroutine(
        self,
        coroutine: Callable[..., Awaitable[CoroutineReturnType]],
        *args: Any,
        **kwargs: Optional[Any],
    ) -> Union[Awaitable[CoroutineReturnType], CoroutineReturnType]:
        """Call coroutine if event-loop is running, call synchronized otherwise.

        Args:
            coroutine: The coroutine to execute either directly or synchronized.

        Returns:
            Awaitable if coroutine was executed directly, result otherwise.
        """
        if self.__loop.is_running():
            return coroutine(*args, **kwargs)
        return self.__loop.run_until_complete(coroutine(*args, **kwargs))

    def __getattribute__(self, name: str) -> Any:
        """Implementation of `__getattribute__` with special case for coroutines.

        Args:
            name: Name of the attribute to read.

        Returns:
            Exactly the same as `__getattribute__` normally does, unless the attribute
            is a coroutine, in which case a wrapped version is returned. Wrapped with
            `__run_coroutine`, to handle sync-calls outside of an active event-loop.
        """
        attribute = getattr(super(), name, None)
        if not attribute:
            return object.__getattribute__(self, name)
        if iscoroutinefunction(attribute):
            return wraps(attribute)(partial(self.__run_coroutine, attribute))
        return attribute
