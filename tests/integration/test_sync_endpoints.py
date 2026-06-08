# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import pytest
from fastramqpi.context import Context
from httpx import AsyncClient
from sqlalchemy import select

from mo_ldap_import_export.ldap_event_generator import LastRun


@pytest.mark.integration_test
@pytest.mark.envvar(
    # Avoid the running poller writing concurrently with us.
    {"LISTEN_TO_CHANGES_IN_LDAP": "False"}
)
async def test_sync_ldap2mo_clears_last_run(
    test_client: AsyncClient,
    context: Context,
) -> None:
    """POST /sync/ldap2mo wipes the LDAP poller's bookkeeping."""
    sessionmaker = context["sessionmaker"]

    async with sessionmaker() as session, session.begin():
        session.add(LastRun(search_base="dc=example,dc=org"))
        session.add(LastRun(search_base="ou=people,dc=example,dc=org"))

    async with sessionmaker() as session, session.begin():
        assert len((await session.scalars(select(LastRun))).all()) == 2

    result = await test_client.post("/sync/ldap2mo")
    assert result.status_code == 200, result.text

    async with sessionmaker() as session, session.begin():
        assert (await session.scalars(select(LastRun))).all() == []
