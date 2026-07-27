# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""TEMPORARY diagnostic instrumentation for the flaky integration-test teardown.

The integration tests intermittently fail at teardown with a ``TimeoutError``
raised from FastRAMQPI's ``run_server`` fixture::

    fastramqpi/pytest_plugin.py: await asyncio.wait_for(task, timeout=300)

The uvicorn task is wedged in ``lifespan.shutdown()`` because the application
lifespan ``__aexit__`` never completes (the GraphQL event fetcher ``TaskGroup``
does not converge). We have been unable to reproduce the exact non-convergence
locally, so this module wraps ``run_server`` with a watchdog that runs *during
teardown* and, if shutdown stalls, dumps the stack of every pending asyncio
task. When the flake next occurs in CI, the job log will show exactly which
coroutine is stuck.

This is a no-op on healthy runs (the watchdog only prints if teardown takes
longer than ``_TICK`` seconds). Remove once the root cause is fixed.
"""

import asyncio
import io
from contextlib import asynccontextmanager
from contextlib import suppress

import fastramqpi.pytest_plugin as _plugin

# Dump every this many seconds while a teardown is stalled.
_TICK = 30.0

_orig_run_server = _plugin.run_server


def _dump_pending_tasks(tag: str, skip: asyncio.Task | None) -> None:
    tasks = [
        t
        for t in asyncio.all_tasks()
        if not t.done() and t is not skip and t is not asyncio.current_task()
    ]
    header = (
        f"\n===== FLAKY-WATCHDOG {tag}: {len(tasks)} pending asyncio task(s) =====\n"
    )
    buf = io.StringIO()
    for t in tasks:
        coro = t.get_coro()
        qual = getattr(coro, "__qualname__", repr(coro))
        buf.write(
            f"\n--- task={t.get_name()} coro={qual} cancelling={t.cancelling()} ---\n"
        )
        # print_stack renders the suspended coroutine's own frames.
        t.print_stack(file=buf)
    # Single write so the dump is not interleaved with other output.
    print(header + buf.getvalue(), flush=True)


@asynccontextmanager
async def _instrumented_run_server(app):  # type: ignore[no-untyped-def]
    watchdog: asyncio.Task | None = None

    async def _watchdog() -> None:
        n = 0
        while True:
            await asyncio.sleep(_TICK)
            n += 1
            _dump_pending_tasks(f"teardown stalled >{int(n * _TICK)}s", skip=watchdog)

    try:
        async with _orig_run_server(app):
            # Only arm the watchdog for the teardown phase, so healthy test
            # bodies pay nothing. The task runs on the loop while the wrapped
            # run_server's __aexit__ awaits the (possibly stuck) uvicorn task.
            yield
            watchdog = asyncio.create_task(_watchdog(), name="flaky-teardown-watchdog")
    finally:
        if watchdog is not None:
            watchdog.cancel()
            with suppress(asyncio.CancelledError):
                await watchdog


# `run_server` is looked up as a module global by the `server` fixture at call
# time, so patching the attribute is sufficient.
_plugin.run_server = _instrumented_run_server
