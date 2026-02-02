# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0

from unittest.mock import patch

import pytest
from httpx import AsyncClient

from fastramqpi.main import FastRAMQPI
from mo_ldap_import_export.utils import combine_dn_strings

@pytest.mark.envvar({
    "LISTEN_TO_CHANGES_IN_LDAP": "False",
    "LISTEN_TO_CHANGES_IN_MO": "False",
})
@pytest.mark.integration_test
async def test_ldap_sync_performance(
    fastramqpi: FastRAMQPI,
    test_client: AsyncClient,
    trigger_ldap_sync,
    add_ldap_person,
    dn2uuid,
) -> None:
    # Use unique person to avoid clashing with mo_person fixture
    person_dn_list = await add_ldap_person("perf1", "0101990001")
    person_dn = combine_dn_strings(person_dn_list)
    ldap_person_uuid = await dn2uuid(person_dn)

    # Ensure app is started and context is populated
    ldap_connection = fastramqpi._context["user_context"]["ldap_connection"]
    
    # We spy on the underlying ldap3 Connection.search method
    original_search = ldap_connection.search
    
    with patch.object(ldap_connection, 'search', side_effect=original_search) as mock_search:
        await trigger_ldap_sync(ldap_person_uuid)
        
        # We want to eventually bring this down to 1.
        assert mock_search.call_count == 1
