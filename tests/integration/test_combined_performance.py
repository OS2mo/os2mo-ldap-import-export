# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0

from unittest.mock import patch
from fastramqpi.main import FastRAMQPI
from mo_ldap_import_export.types import LDAPUUID
from httpx import AsyncClient
import pytest
import json

@pytest.mark.envvar(
    {
        "CONVERSION_MAPPING": json.dumps({
            "ldap_to_mo": {
                "Employee": {
                    "objectClass": "Employee",
                    "_import_to_mo_": "true",
                    "_ldap_attributes_": ["employeeNumber", "title"],
                    "uuid": "{{ employee_uuid }}",
                    "cpr_number": "{{ldap.employeeNumber}}",
                    "user_key": "{{ ldap.title }}"
                }
            },
            "ldap_to_mo_any": {
                "inetOrgPerson": [{
                    "objectClass": "Class",
                    "_import_to_mo_": "true",
                    "_ldap_attributes_": ["uid"],
                    "uuid": "00000000-0000-0000-0000-000000000000", # Dummy UUID
                    "user_key": "{{ ldap.uid }}",
                    "name": "Test Class",
                    "facet": "00000000-0000-0000-0000-000000000001",
                    "scope": "Test Scope"
                }]
            }
        })
    }
)
@pytest.mark.integration_test
async def test_combined_ldap_sync_performance(
    fastramqpi: FastRAMQPI,
    test_client: AsyncClient,
    trigger_ldap_sync,
    ldap_person_uuid: LDAPUUID,
) -> None:
    # Ensure app is started and context is populated
    ldap_connection = fastramqpi._context["user_context"]["ldap_connection"]
    
    # We spy on the underlying ldap3 Connection.search method
    original_search = ldap_connection.search
    
    with patch.object(ldap_connection, 'search', side_effect=original_search) as mock_search:
        await trigger_ldap_sync(ldap_person_uuid)
        
        # Expected:
        # 1. http_process_uuid initial fetch (1 query).
        # 2. import_single_user (skipped sibling search as discriminator is off).
        # 3. import_single_object_class (reuses object).
        
        print(f"LDAP Search called {mock_search.call_count} times")
        assert mock_search.call_count == 1
