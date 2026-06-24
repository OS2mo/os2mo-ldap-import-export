# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from collections.abc import Awaitable
from collections.abc import Callable
from typing import TypeAlias
from uuid import UUID

import pytest
from httpx import AsyncClient

from mo_ldap_import_export.ldapapi import LDAPAPI
from mo_ldap_import_export.types import LDAPUUID
from mo_ldap_import_export.utils import combine_dn_strings
from tests.integration.conftest import AddLdapPerson

EmitSubtree: TypeAlias = Callable[..., Awaitable[set[LDAPUUID]]]


@pytest.fixture
async def emit_subtree(test_client: AsyncClient) -> EmitSubtree:
    async def inner(search_base: str, **params: str | bool) -> set[LDAPUUID]:
        result = await test_client.request(
            "GET",
            "/ldap_event_generator/since",
            content=search_base,
            headers={"Content-Type": "text/plain"},
            params=params,
        )
        assert result.status_code == 200, result.text
        return {LDAPUUID(u) for u in result.json()}

    return inner


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
async def test_changed_since(emit_subtree: EmitSubtree, ldap_person_uuid: UUID) -> None:
    base = "ou=os2mo,o=magenta,dc=magenta,dc=dk"
    # A `since` predating the person returns it.
    assert await emit_subtree(base, since="2000-01-01T00:00:00Z") == {ldap_person_uuid}
    # A `since` postdating the person excludes it.
    assert await emit_subtree(base, since="2100-01-01T00:00:00Z") == set()


@pytest.mark.integration_test
@pytest.mark.envvar(
    {"LISTEN_TO_CHANGES_IN_MO": "False", "LISTEN_TO_CHANGES_IN_LDAP": "False"}
)
async def test_changed_since_pagination(
    emit_subtree: EmitSubtree,
    add_ldap_person: AddLdapPerson,
    ldap_api: LDAPAPI,
) -> None:
    uuids = set()
    for x in range(2000):
        ldap_person = await add_ldap_person(str(x), "010170" + str(x).rjust(4, "0"))
        person_dn = combine_dn_strings(ldap_person)
        uuids.add(await ldap_api.get_ldap_unique_ldap_uuid(person_dn))

    returned = await emit_subtree(
        "ou=os2mo,o=magenta,dc=magenta,dc=dk", since="2000-01-01T00:00:00Z"
    )
    assert returned == uuids
