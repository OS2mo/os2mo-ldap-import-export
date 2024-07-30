# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
from datetime import datetime
from datetime import timezone

import pytest
from fastramqpi.context import Context
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker

from mo_ldap_import_export.ldap_event_generator import LastRun
from mo_ldap_import_export.ldap_event_generator import update_timestamp


async def num_last_run_entries(sessionmaker: async_sessionmaker[AsyncSession]) -> int:
    async with sessionmaker() as session, session.begin():
        result = await session.execute(select(LastRun))
        return len(result.fetchall())


@pytest.mark.integration_test
@pytest.mark.envvar(
    # If we are listening to changes in LDAP it will write concurrently with us
    {"LISTEN_TO_CHANGES_IN_LDAP": "False"}
)
@pytest.mark.usefixtures("test_client")
async def test_update_timestamp_postgres(context: Context) -> None:
    sessionmaker = context["sessionmaker"]

    test_start = datetime.now(timezone.utc)
    assert await num_last_run_entries(sessionmaker) == 0

    async with update_timestamp(sessionmaker, "dc=ad1") as last_run:
        assert last_run == datetime.min.replace(tzinfo=timezone.utc)
    assert await num_last_run_entries(sessionmaker) == 1

    async with update_timestamp(sessionmaker, "dc=ad1") as last_run:
        assert last_run > test_start
    assert await num_last_run_entries(sessionmaker) == 1

    async with update_timestamp(sessionmaker, "dc=ad2") as last_run:
        assert last_run == datetime.min.replace(tzinfo=timezone.utc)
    assert await num_last_run_entries(sessionmaker) == 2

    async with update_timestamp(sessionmaker, "dc=ad2") as last_run:
        assert last_run > test_start
    assert await num_last_run_entries(sessionmaker) == 2
