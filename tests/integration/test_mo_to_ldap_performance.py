# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0

from unittest.mock import patch
from fastramqpi.main import FastRAMQPI
from mo_ldap_import_export.types import EmployeeUUID
from httpx import AsyncClient
import pytest
from fastramqpi.pytest_util import retrying
from more_itertools import one

@pytest.mark.integration_test
async def test_mo_to_ldap_sync_performance(
    fastramqpi: FastRAMQPI,
    test_client: AsyncClient,
    trigger_sync,
    ldap_person: list[str], 
) -> None:
    # Ensure app is started and context is populated
    ldap_connection = fastramqpi._context["user_context"]["ldap_connection"]
    graphql_client = fastramqpi._context["graphql_client"]

    # Find the MO employee created by ldap_person sync (or wait for it)
    mo_uuid = None
    async for attempt in retrying():
        with attempt:
            res = await graphql_client.read_employee_uuid_by_cpr_number(
                cpr_number="2108613133"
            )
            if res.objects:
                mo_uuid = one(res.objects).uuid
    
    assert mo_uuid is not None

    # We spy on the underlying ldap3 Connection.search method
    original_search = ldap_connection.search
    
    with patch.object(ldap_connection, 'search', side_effect=original_search) as mock_search:
        await trigger_sync(mo_uuid)
        
        print(f"LDAP Search called {mock_search.call_count} times")
        
        # Baseline check (uncomment to see count)
        assert mock_search.call_count == 1
