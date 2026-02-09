# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import asyncio
import logging
import os
import urllib.parse
from asyncio import CancelledError
from asyncio import create_task
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextlib import suppress
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Iterator
from typing import Mapping
from typing import NoReturn
from unittest.mock import patch

# WARNING: You CANNOT import from `fastramqpi` here!
#
# This file is registered as a pytest plugin in pyproject.toml. Pytest does not
# allow controlling the order in which plugins are loaded, so if this plugin is
# loaded before pytest-cov, this file *and every line that was executed from
# its imports* is reported as missed because it doesn't execute after coverage
# collection is started.
# https://github.com/pytest-dev/pytest-cov/issues/635
# https://github.com/pytest-dev/pytest-cov/issues/682
# https://pytest-cov.readthedocs.io/en/latest/plugins.html
import httpx
import pytest
import sqlalchemy
import structlog
import uvicorn
from fastapi import FastAPI
from httpx import AsyncClient
from httpx import BasicAuth
from more_itertools import one
from pytest import Config
from pytest import FixtureRequest
from pytest import Item
from pytest import MonkeyPatch
from respx import MockRouter
from sqlalchemy import Connection
from sqlalchemy import text

# WARNING: Do not import from `fastramqpi` here!


logger = structlog.stdlib.get_logger()


def pytest_configure(config: Config) -> None:
    config.addinivalue_line(
        "markers", "integration_test: mark test as an integration test."
    )
    config.addinivalue_line(
        "markers", "envvar(mapping): set the specified environmental variables"
    )


def pytest_collection_modifyitems(items: list[Item]) -> None:
    """Automatically use convenient fixtures for tests marked with integration_test."""

    for item in items:
        if item.get_closest_marker("integration_test"):
            # MUST prepend to replicate auto-use fixtures coming first
            item.fixturenames[:0] = [  # type: ignore[attr-defined]
                "passthrough_backing_services",
                "fastramqpi_database_setup",
                "fastramqpi_database_isolation",
                "os2mo_database_snapshot_and_restore",
                "amqp_queue_isolation",
                "amqp_event_emitter",
                "graphql_events_quick_fetch",
                "graphql_events_quick_retry",
            ]
        else:  # unit-test
            # MUST prepend to replicate auto-use fixtures coming first
            item.fixturenames[:0] = [  # type: ignore[attr-defined]
                "empty_environment",
            ]


@pytest.fixture
async def empty_environment() -> AsyncIterator[None]:
    """Clear all environmental variables before running unit-test."""
    with patch.dict(os.environ, clear=True):
        yield


@pytest.fixture(autouse=True)
def load_marked_envvars(
    monkeypatch: pytest.MonkeyPatch,
    request: FixtureRequest,
) -> Iterator[None]:
    """Fixture to inject environmental variable via pytest.marks.

    Example:
        ```
        @pytest.mark.envvar({"VAR1": "1", "VAR2": 2})
        @pytest.mark.envvar({"VAR3": "3"})
        def test_load_marked_envvars() -> None:
            assert os.environ.get("VAR1") == "1"
            assert os.environ.get("VAR2") == "2"
            assert os.environ.get("VAR3") == "3"
            assert os.environ.get("VAR4") is None
        ```

    Args:
        monkeypatch: The patcher to use for settings the environmental variables.
        request: The pytest request object used to extract markers.

    Yields:
        None, but keeps the settings overrides active.
    """
    envvars: dict[str, str] = {}
    for mark in request.node.iter_markers("envvar"):
        if not mark.args:
            pytest.fail("envvar mark must take an argument")
        if len(mark.args) > 1:
            pytest.fail("envvar mark must take at most one argument")
        argument = one(mark.args)
        if not isinstance(argument, Mapping):
            pytest.fail("envvar mark argument must be a mapping")
        if any(not isinstance(key, str) for key in argument.keys()):
            pytest.fail("envvar mapping keys must be strings")
        if any(not isinstance(value, str) for value in argument.values()):
            pytest.fail("envvar mapping values must be strings")
        envvars.update(**argument)
    for key, value in envvars.items():
        monkeypatch.setenv(key, value)
    yield


@pytest.fixture(scope="session")
def _settings() -> Any:
    """Access FastRAMQPI settings without coupling to the integration's settings."""
    # We must defer importing from the FastRAMQPI module till run-time.
    # https://github.com/pytest-dev/pytest-cov/issues/587
    from fastramqpi.config import Settings

    class _Settings(Settings):
        class Config:
            env_prefix = "FASTRAMQPI__"

    return _Settings()


@pytest.fixture
async def unauthenticated_mo_client(_settings: Any) -> AsyncIterator[AsyncClient]:
    """HTTPX client with the OS2mo URL preconfigured."""
    mo_client = AsyncClient(base_url=_settings.mo_url)
    async with mo_client as client:
        yield client


@pytest.fixture
async def mo_client(_settings: Any) -> AsyncIterator[AsyncClient]:
    """HTTPX client with the OS2mo URL and auth preconfigured.

    For use by the integration's fixtures and tests. May be closed by the
    integration's ariadne codegen client, and as such cannot be used for
    post-test teardown (such as database restore).
    """
    from fastramqpi.main import construct_mo_client

    async with construct_mo_client(_settings) as client:
        yield client


@pytest.fixture
async def app() -> FastAPI:
    raise NotImplementedError(
        "You need to override the `app` fixture. See the FastRAMQPI README."
    )


def _is_uvicorn_cancellederror(record: logging.LogRecord) -> bool:
    if record.exc_info is None:
        return False
    exc_type, exc_value, exc_traceback = record.exc_info
    # https://github.com/Kludex/uvicorn/blob/918dae6ef91917a5824ab48c46026e07b7c80beb/uvicorn/server.py#L289
    if not isinstance(exc_value, CancelledError):
        return False
    if str(exc_value) != "Task cancelled, timeout graceful shutdown exceeded":
        return False
    logger.warning("Ignoring uvicorn cancellation errors")
    return True


@asynccontextmanager
async def run_server(app: FastAPI) -> AsyncIterator[None]:
    """Construct an ASGI server on localhost port 8000."""
    # Start uvicorn
    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=8000,
        # When uvicorn is asked to shutdown, it first stops accepting new http
        # connections, and then waits for ongoing ones to finish, potentially
        # forever. Since the test has already passed (or failed!), we don't
        # want to spend any time waiting around.
        timeout_graceful_shutdown=0,
    )
    server = uvicorn.Server(config)
    task = None
    try:
        task = asyncio.create_task(server.serve())
        async with asyncio.timeout(60):
            while not server.started:
                await asyncio.sleep(0.1)
        # Yield the running server
        yield
    finally:
        # Uvicorn will throw a lot of errors when ongoing requests are
        # cancelled due to timeout_graceful_shutdown=0, ignore them.
        logging.getLogger("uvicorn.error").addFilter(
            lambda record: not _is_uvicorn_cancellederror(record)
        )

        # Always stop uvicorn whether our app exited successfully or not
        # The reason this is required here while being omitted for the other code in
        # this file, is that this is a standard asynccontextmanager while the functions
        # are pytest fixtures, for which pytest handles the exceptions
        server.should_exit = True

        # When uvicorn is asked to shutdown, it does the following:
        #  1. "Shutting down" (uvicorn stops accepting new http connections)
        #  2. "Waiting for connections to close"
        #  3. "Waiting for application shutdown" (uvicorn stops the lifespan)
        #  4. "Application shutdown complete"
        #  5. "Finished server process"
        # The duration of step 2 is controlled by the timeout_graceful_shutdown
        # option above, but step 3 might still take some time.
        assert task is not None
        await asyncio.wait_for(task, timeout=300)


@pytest.fixture
async def server(app: FastAPI) -> AsyncIterator[None]:
    async with run_server(app):
        yield


@asynccontextmanager
async def run_test_client() -> AsyncIterator[AsyncClient]:
    """Create httpx client bound to localhost port 8000."""
    # Yield HTTPX client for localhost
    async with AsyncClient(
        base_url="http://127.0.0.1:8000",
        # To avoid flaky tests, Starlette's TestClient does not enforce
        # timeouts[1]. We subscribe to their philosophy, but set a timeout of 5
        # minutes to avoid stuck tests.
        # [1] https://github.com/encode/starlette/issues/1108
        timeout=300,
    ) as client:
        yield client


@pytest.fixture
async def test_client(server: None) -> AsyncIterator[AsyncClient]:
    """Create httpx client bound to the local uvicorn."""
    async with run_test_client() as client:
        yield client


@pytest.fixture
async def rabbitmq_management_client(_settings: Any) -> AsyncIterator[AsyncClient]:
    """HTTPX client for the RabbitMQ management API."""
    amqp = _settings.amqp.get_url()
    async with httpx.AsyncClient(
        base_url=f"http://{amqp.host}:15672/api/",
        auth=BasicAuth(
            username=amqp.user,
            password=amqp.password,
        ),
    ) as client:
        yield client


@pytest.fixture(scope="session")
def superuser(_settings: Any) -> Iterator[Connection]:
    """Managing databases requires a superuser connection."""
    # Connect to "postgres" since we cannot drop a database while being connected to it.
    # TODO: it would be easier to use our own create_engine() from the database module,
    # but pytest cannot properly share event-loops across session-scoped fixtures and
    # (function-scoped) tests. Therefore, we use sqlalchemy's *sync* engine instead.
    # https://github.com/pytest-dev/pytest-asyncio/issues/706#issuecomment-1838860535
    db = _settings.database
    url = f"postgresql+psycopg://{db.user}:{db.password}@{db.host}:{db.port}/postgres"
    engine = sqlalchemy.create_engine(url)
    # AUTOCOMMIT disables transactions to allow for create/drop database operations
    engine.update_execution_options(isolation_level="AUTOCOMMIT")
    with engine.begin() as connection:
        yield connection


@pytest.fixture(scope="session")
def fastramqpi_database_setup(superuser: Connection) -> None:
    """Set up testing database template."""
    # Create separate testing template database. We will apply the database migrations
    # to this database once, and then use a copy of it for each test.
    template_db = "test_template"
    superuser.execute(text(f"drop database if exists {template_db}"))
    superuser.execute(text(f"create database {template_db}"))
    # Run migrations
    # TODO: alembic isn't implemented yet so we don't have to do anything here. Tables
    # are created on app startup for now.


@pytest.fixture
def fastramqpi_database_isolation(
    superuser: Connection, monkeypatch: MonkeyPatch
) -> None:
    """Ensure test isolation by resetting the database between tests.

    Automatically used on tests marked as integration_test.
    """
    # Copy template testing database (with migrations applied) to a temporary testing
    # database for the test that's about to run.
    template_db = "test_template"
    test_db = "test"
    superuser.execute(
        text(
            f"""
            select pg_terminate_backend(pid)
            from pg_stat_activity
            where datname = '{test_db}' and pid <> pg_backend_pid()
            """
        )
    )
    superuser.execute(text(f"drop database if exists {test_db}"))
    superuser.execute(text(f"create database {test_db} template {template_db}"))
    # Patch environment so the app under test will connect to this temporary database
    monkeypatch.setenv("FASTRAMQPI__DATABASE__NAME", test_db)


@pytest.fixture
async def amqp_emit_events(
    unauthenticated_mo_client: AsyncClient,
) -> Callable[[], Awaitable[None]]:
    """Emits outstanding events when called."""

    async def inner() -> None:
        r = await unauthenticated_mo_client.post("/testing/amqp/emit")
        r.raise_for_status()

    return inner


@pytest.fixture
async def amqp_event_emitter(
    amqp_emit_events: Callable[[], Awaitable[None]],
) -> AsyncIterator[None]:
    """Continuously, and quickly, emit OS2mo AMQP events during tests.

    Normally, OS2mo emits AMQP events periodically, but very slowly. Even though there
    are no guarantees as to message delivery speed, and we therefore should not design
    our system around such expectation, waiting a long time for tests to pass in the
    pipelines - or to fail during development - is a very poor development experience.

    Automatically used on tests marked as integration_test.
    """

    async def emitter() -> NoReturn:
        while True:
            await asyncio.sleep(1.13)
            await amqp_emit_events()

    task = create_task(emitter())
    yield
    task.cancel()
    with suppress(CancelledError):
        await task


@pytest.fixture
async def graphql_events_quick_retry(
    unauthenticated_mo_client: AsyncClient,
) -> AsyncIterator[None]:
    """Continuously reset GraphQL events to allow quick retrying during tests.

    Normally, events which are fetched - but not acknowledged - are not retried
    (cannot be fetched) for three minutes, increasing exponentially.

    Automatically used on tests marked as integration_test.
    """

    async def resetter() -> NoReturn:
        while True:
            await asyncio.sleep(0.941)
            r = await unauthenticated_mo_client.post("/testing/events/reset-last-tried")
            r.raise_for_status()

    task = create_task(resetter())
    yield
    task.cancel()
    with suppress(CancelledError):
        await task


@pytest.fixture
async def graphql_events_quick_fetch(monkeypatch: MonkeyPatch) -> None:
    """Quickly fetch GraphQL events during tests."""
    monkeypatch.setattr("fastramqpi.events.NO_EVENT_SLEEP_DURATION", 0.599)


@pytest.fixture
async def os2mo_database_snapshot_and_restore(
    unauthenticated_mo_client: AsyncClient,
) -> AsyncIterator[None]:
    """Ensure test isolation by resetting the OS2mo database between tests.

    Automatically used on tests marked as integration_test.
    """
    r = await unauthenticated_mo_client.post("/testing/database/snapshot")
    r.raise_for_status()
    yield
    r = await unauthenticated_mo_client.post("/testing/database/restore")
    r.raise_for_status()


@pytest.fixture
async def amqp_queue_isolation(
    rabbitmq_management_client: AsyncClient,
) -> None:
    """Ensure test isolation by deleting all AMQP queues before tests.

    Automatically used on tests marked as integration_test.
    """
    queues = (await rabbitmq_management_client.get("queues")).json()
    # vhost and name must be URL-encoded. This includes `/`, which is normally regarded
    # as safe. This is particularly important for the default AMQP vhost `/`.
    urls = (
        "queues/{vhost}/{name}".format(
            vhost=urllib.parse.quote(q["vhost"], safe=""),
            name=urllib.parse.quote(q["name"], safe=""),
        )
        for q in queues
    )
    deletes = [rabbitmq_management_client.delete(url) for url in urls]
    await asyncio.gather(*deletes)


@pytest.fixture
def passthrough_backing_services(_settings: Any, respx_mock: MockRouter) -> None:
    """Allow calls to the backing services to bypass the RESPX mocking.

    Automatically used on tests marked as integration_test.
    """
    # the integration itself, running in uvicorn
    respx_mock.route(host="127.0.0.1", port=8000).pass_through()
    # mo and keycloak are named to allow tests to revert the passthrough if needed
    respx_mock.route(name="keycloak", host=_settings.auth_server.host).pass_through()
    respx_mock.route(name="mo", host=_settings.mo_url.host).pass_through()
    # rabbitmq management
    respx_mock.route(host=_settings.amqp.get_url().host).pass_through()
    respx_mock.route(host="localhost").pass_through()


@pytest.fixture
def get_num_queued_messages(
    rabbitmq_management_client: AsyncClient,
) -> Callable[[], Awaitable[int]]:
    """Get number of queued messages in RabbitMQ AMQP."""

    async def _get_num_queued_messages() -> int:
        queues = (await rabbitmq_management_client.get("queues")).json()
        return sum(
            queue.get("messages_ready", 0) + queue.get("messages_unacknowledged", 0)
            for queue in queues
        )

    return _get_num_queued_messages


@pytest.fixture
def get_num_consumed_messages(
    rabbitmq_management_client: AsyncClient,
) -> Callable[[], Awaitable[int]]:
    """Get number of consumed messages in RabbitMQ AMQP."""

    async def _get_num_consumed_messages() -> int:
        queues = (await rabbitmq_management_client.get("queues")).json()
        return sum(queue.get("message_stats", {}).get("ack", 0) for queue in queues)

    return _get_num_consumed_messages
