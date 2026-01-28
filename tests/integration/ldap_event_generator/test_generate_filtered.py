# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0

import pytest
from httpx import AsyncClient

from mo_ldap_import_export.ldap_event_generator import MICROSOFT_EPOCH
from mo_ldap_import_export.types import LDAPUUID
from mo_ldap_import_export.utils import combine_dn_strings


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_LDAP": "True",
        "LISTEN_TO_CHANGES_IN_MO": "False",
    }
)
async def test_generate_filtered(
    test_client: AsyncClient,
    ldap_person_uuid: LDAPUUID,
    ldap_org: list[str],
) -> None:
    # Use the internal endpoint to fetch changes instead of waiting for the poller
    # This proves that the poll logic itself is filtering correctly
    since = MICROSOFT_EPOCH.isoformat()
    search_base = combine_dn_strings(ldap_org)
    response = await test_client.request(
        "GET",
        f"/ldap_event_generator/{since}",
        content=search_base,
        headers={"Content-Type": "text/plain"},
    )
    assert response.status_code == 200

    # The ldap_person_uuid fixture ensures an inetOrgPerson exists.
    # It also creates an organizationalUnit and an organization.
    # We all of them to be present here.
    uuids = {LDAPUUID(u) for u in response.json()}
    assert uuids == {ldap_person_uuid}
