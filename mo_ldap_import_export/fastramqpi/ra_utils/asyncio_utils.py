# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from asyncio import Semaphore
from asyncio import gather
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Awaitable
from typing import Callable
from typing import List
from typing import TypeVar
from typing import cast

ReturnType = TypeVar("ReturnType")
WithConcurrencyFunction = Callable[..., Awaitable[ReturnType]]


def with_concurrency(
    parallel: int,
) -> Callable[[WithConcurrencyFunction], WithConcurrencyFunction]:
    """Decorator which limits concurrency.

    Example:
        ```Python
        @with_concurrency(5)
        async def intensive_task(i: int) -> None:
            ...
            return i

        tasks = list(map(intensive_task, range(1000)))

        # Runs 5 intensive_tasks in parallel
        await asyncio.gather(*tasks)
        ```

    Args:
        parallel: The number of concurrent tasks being executed (must be positive).

    Raises:
        TypeError: if parallel has the wrong type.
        ValueError: if parallel is negative.

    Returns:
        Decorator function to limit concurrency.
    """
    semaphore = Semaphore(parallel)

    @asynccontextmanager
    async def limited_concurrency() -> AsyncGenerator[None, None]:
        async with semaphore:
            yield

    return limited_concurrency()


async def gather_with_concurrency(
    parallel: int, *tasks: Awaitable[ReturnType]
) -> List[ReturnType]:
    """Asyncio gather, but with limited concurrency.

    Example:
        ```Python
        async def intensive_task(i: int) -> None:
            ...
            return i

        tasks = list(map(intensive_task, range(1000)))

        # Runs 1000 intensive_tasks in parallel
        await asyncio.gather(*tasks)
        # Runs at most 5 intensive_tasks in parallel, through all 1000
        await gather_with_concurrency(5, *tasks)
        ```

    Args:
        parallel: The number of concurrent tasks being executed (must be positive).
        tasks: List of tasks to execute.

    Raises:
        TypeError: if parallel has the wrong type.
        ValueError: if parallel is negative.

    Returns:
        List of return values from awaiting the tasks.
    """

    @with_concurrency(parallel)
    async def resolver_task(task: Awaitable[ReturnType]) -> ReturnType:
        return await task

    return cast(List[ReturnType], await gather(*map(resolver_task, tasks)))
