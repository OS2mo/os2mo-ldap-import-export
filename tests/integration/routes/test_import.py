# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from collections.abc import Awaitable
from collections.abc import Callable
from urllib.parse import quote_plus
from uuid import UUID

import pytest
from httpx import AsyncClient
from structlog.testing import capture_logs

from mo_ldap_import_export.utils import combine_dn_strings
from tests.integration.conftest import AddLdapPerson


@pytest.fixture
async def add_ldap_persons(
    test_client: AsyncClient,
    add_ldap_person: AddLdapPerson,
) -> Callable[[int], Awaitable[set[UUID]]]:
    async def adder(num_accounts: int) -> set[UUID]:
        uuids = set()
        for x in range(num_accounts):
            dn = combine_dn_strings(await add_ldap_person(str(x), "2108613133"))
            response = await test_client.get("/Inspect/dn2uuid/" + quote_plus(dn))
            assert response.status_code == 200
            uuid = response.json()
            uuids.add(uuid)
        return uuids

    return adder


@pytest.mark.integration_test
@pytest.mark.parametrize("num_accounts", (0, 1, 5))
async def test_import(
    test_client: AsyncClient,
    add_ldap_persons: Callable[[int], Awaitable[set[UUID]]],
    num_accounts: int,
) -> None:
    uuids = await add_ldap_persons(num_accounts)

    response = await test_client.get("/Import")
    assert response.status_code == 202

    result = response.json()
    assert set(result) == uuids


@pytest.mark.integration_test
async def test_import_only_first_20(
    test_client: AsyncClient,
    add_ldap_persons: Callable[[int], Awaitable[set[UUID]]],
) -> None:
    uuids = await add_ldap_persons(22)

    response = await test_client.get(
        "/Import", params={"test_on_first_20_entries": True}
    )
    assert response.status_code == 202

    result = response.json()
    triggered_uuids = set(result)
    assert len(triggered_uuids) == 20
    assert len(uuids) == 22
    assert uuids.issuperset(triggered_uuids)


@pytest.mark.integration_test
@pytest.mark.envvar({"LDAP_CPR_ATTRIBUTE": "", "LDAP_IT_SYSTEM": "ADUUID"})
async def test_import_cpr_indexed_but_no_cpr_index(test_client: AsyncClient) -> None:
    response = await test_client.get("/Import")
    assert response.status_code == 404
    result = response.json()
    assert result == {"detail": "cpr_field is not configured"}


@pytest.mark.integration_test
async def test_import_bad_cpr_number(test_client: AsyncClient, add_ldap_person) -> None:
    await add_ldap_person("abk", "5001012002")

    with capture_logs() as cap_logs:
        response = await test_client.get("/Import")
        assert response.status_code == 202
        result = response.json()
        assert result == []

    messages = [w for w in cap_logs if w["log_level"] == "info"]
    assert "Invalid CPR Number found" in str(messages)
