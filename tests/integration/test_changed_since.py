# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from uuid import UUID

import pytest
from httpx import AsyncClient

from mo_ldap_import_export.ldapapi import LDAPAPI
from mo_ldap_import_export.utils import combine_dn_strings
from tests.integration.conftest import AddLdapPerson


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
async def test_changed_since(test_client: AsyncClient, ldap_person_uuid: UUID) -> None:
    content = "ou=os2mo,o=magenta,dc=magenta,dc=dk"
    headers = {"Content-Type": "text/plain"}
    result = await test_client.request(
        "GET",
        "/ldap_event_generator/since",
        params={"since": "2000-01-01T00:00:00Z"},
        content=content,
        headers=headers,
    )
    assert result.status_code == 200
    assert set(result.json()) == {str(ldap_person_uuid)}


@pytest.mark.integration_test
@pytest.mark.envvar(
    {"LISTEN_TO_CHANGES_IN_MO": "False", "LISTEN_TO_CHANGES_IN_LDAP": "False"}
)
async def test_changed_since_pagination(
    test_client: AsyncClient,
    add_ldap_person: AddLdapPerson,
    ldap_api: LDAPAPI,
) -> None:
    uuids = set()
    for x in range(2000):
        ldap_person = await add_ldap_person(str(x), "010170" + str(x).rjust(4, "0"))
        person_dn = combine_dn_strings(ldap_person)
        uuids.add(await ldap_api.get_ldap_unique_ldap_uuid(person_dn))

    content = "ou=os2mo,o=magenta,dc=magenta,dc=dk"
    headers = {"Content-Type": "text/plain"}
    result = await test_client.request(
        "GET",
        "/ldap_event_generator/since",
        params={"since": "2000-01-01T00:00:00Z"},
        content=content,
        headers=headers,
    )
    assert result.status_code == 200
    result_uuids = set(map(UUID, result.json()))
    assert len(result_uuids) == len(uuids)
    assert result_uuids == uuids
