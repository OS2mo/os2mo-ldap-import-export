# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import sqlalchemy
from pydantic import BaseSettings
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from fastramqpi.config import DatabaseSettings


def create_engine(
    user: str,
    password: str,
    host: str,
    port: int,
    name: str,
) -> AsyncEngine:
    url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{name}"
    return create_async_engine(url)


def create_sessionmaker(engine: AsyncEngine) -> async_sessionmaker:
    return async_sessionmaker(engine)


def run_upgrade(database_metadata: MetaData) -> None:
    """Apply alembic migrations (in the future).

    TODO: when we implement alembic, the caller should no longer pass us their
    metadata; all of the table definitions will be apparent from the migrations in each
    integration (which will be available to alembic).
    In fact, we probably don't even want to run migrations during app startup, but
    delegate it to a CLI interface or similar, which can be run externally to the main
    process. In this case, we still need to implement programmatic migrations, to
    implement the CLI in python and run migrations from the test suite.

    These cookbooks could be useful (and are used in OS2mo's setup):
    https://alembic.sqlalchemy.org/en/latest/cookbook.html#programmatic-api-use-connection-sharing-with-asyncio
    https://alembic.sqlalchemy.org/en/latest/cookbook.html#connection-sharing
    https://alembic.sqlalchemy.org/en/latest/cookbook.html#asyncio-recipe
    """

    class Settings(DatabaseSettings, BaseSettings):
        """Load database variables without depending on the integration's settings."""

        class Config:
            env_prefix = "FASTRAMQPI__DATABASE__"

    # TODO: when we implement alembic the whole interface will be async and we can use
    # our own create_engine() from above.
    db = Settings()
    url = f"postgresql+psycopg://{db.user}:{db.password}@{db.host}:{db.port}/{db.name}"
    engine = sqlalchemy.create_engine(url)
    # Create all tables in the metadata, ignoring tables already present in the
    # database. A proper migration tool, such as alembic, is more appropriate.
    # https://docs.sqlalchemy.org/en/20/tutorial/metadata.html#emitting-ddl-to-the-database
    with engine.begin() as connection:
        database_metadata.create_all(connection)
