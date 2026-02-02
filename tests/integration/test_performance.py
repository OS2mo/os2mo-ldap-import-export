# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0

from unittest.mock import patch
from fastramqpi.main import FastRAMQPI
from mo_ldap_import_export.types import LDAPUUID
from httpx import AsyncClient
import pytest

@pytest.mark.integration_test
async def test_ldap_sync_performance(
    fastramqpi: FastRAMQPI,
    test_client: AsyncClient,
    trigger_ldap_sync,
    ldap_person_uuid: LDAPUUID,
) -> None:
    # Ensure app is started and context is populated
    ldap_connection = fastramqpi._context["user_context"]["ldap_connection"]
    
    # We spy on the underlying ldap3 Connection.search method
    # It is a synchronous method called via asyncio.to_thread in the application
    original_search = ldap_connection.search
    
    with patch.object(ldap_connection, 'search', side_effect=original_search) as mock_search:
        await trigger_ldap_sync(ldap_person_uuid)
        
        # Current implementation does multiple lookups:
        # 1. get_object_by_uuid (to get objectClass and DN) -> single_object_search -> object_search -> ldap_search
        # 2. get_ldap_dn (inside http_reconcile_uuid? No, we are hitting http_process_uuid)
        
        # In http_process_uuid:
        # 1. get_object_by_uuid (uuid -> object)
        # 2. import_single_user -> find_mo_employee_uuid (dn -> uuid) - might involve search if by cpr? 
        #    But here we have existing user.
        #    Wait, find_mo_employee_uuid calls dataloader which might use ldapapi.dn2cpr which does a search.
        # 3. filter_dns (might search)
        # 4. apply_discriminator (might search)
        # 5. import_single_entity -> get_ldap_object (search)
        
        # We want to reduce the number of searches.
        # Let's fail if it's too high.
        # I suspect it is around 4-5.
        
        print(f"LDAP Search called {mock_search.call_count} times")
        
        # We want to eventually bring this down to 1 or 2.
        # For now let's assert strictly to see the current count.
        assert mock_search.call_count == 3
