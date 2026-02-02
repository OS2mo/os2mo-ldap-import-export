# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0

import json
from unittest.mock import patch

import pytest
from httpx import AsyncClient

from fastramqpi.main import FastRAMQPI
from mo_ldap_import_export.utils import combine_dn_strings

@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "CONVERSION_MAPPING": json.dumps({
            "ldap_to_mo": {
                "Employee": {
                    "objectClass": "Employee",
                    "_import_to_mo_": "true",
                    "_ldap_attributes_": ["employeeNumber", "title", "cn", "sn"],
                    "uuid": "{{ employee_uuid }}",
                    "cpr_number": "{{ldap.employeeNumber}}",
                    "user_key": "{{ ldap.title }}",
                    "given_name": "{{ ldap.cn[0] }}",
                    "surname": "{{ ldap.sn[0] }}"
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
    add_ldap_person,
    dn2uuid,
) -> None:
    # Use unique person to avoid clashing with mo_person fixture
    person_dn_list = await add_ldap_person("perf3", "0101990003")
    person_dn = combine_dn_strings(person_dn_list)
    ldap_person_uuid = await dn2uuid(person_dn)

    # Ensure app is started and context is populated
    ldap_connection = fastramqpi._context["user_context"]["ldap_connection"]

    # We spy on the underlying ldap3 Connection.search method
    original_search = ldap_connection.search

    with patch.object(ldap_connection, 'search', side_effect=original_search) as mock_search:
        await trigger_ldap_sync(ldap_person_uuid)
        
        # Combined handler should still only result in 1 query if we pass the object down.
        assert mock_search.call_count == 1
