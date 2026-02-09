# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import asyncio
from functools import wraps
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Optional
from typing import TypeVar

CallableReturnType = TypeVar("CallableReturnType")


def async_to_sync(
    func: Callable[..., Awaitable[CallableReturnType]],
) -> Callable[..., CallableReturnType]:
    """Function decorator to run an async function to completion.

    Example:
        ```Python
        @async_to_sync
        async def sleepy(seconds):
            await asyncio.sleep(seconds)
            return seconds

        print(sleepy(5))  # --> 5
        ```

    Args:
        func: The asynchronous function to wrap.

    Returns:
        The newly generated synchronous function wrapping the async one.
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Optional[Any]) -> CallableReturnType:
        return asyncio.run(func(*args, **kwargs))  # type: ignore

    return wrapper
