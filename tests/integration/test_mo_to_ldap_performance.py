# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0

import json
from unittest.mock import patch

import pytest
from httpx import AsyncClient
from more_itertools import one

from fastramqpi.main import FastRAMQPI
from fastramqpi.pytest_util import retrying
from mo_ldap_import_export.utils import combine_dn_strings

@pytest.mark.envvar({
    "LISTEN_TO_CHANGES_IN_LDAP": "False",
    "LISTEN_TO_CHANGES_IN_MO": "True",
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
        "mo2ldap": """
            {% set mo = load_mo_employee(uuid) %}
            {{
                {
                    "employeeNumber": mo.cpr_number,
                    "title": mo.uuid|string,
                    "givenName": mo.given_name,
                    "sn": mo.surname,
                }|tojson
            }}
        """
    })
})
@pytest.mark.integration_test
async def test_mo_to_ldap_sync_performance(
    fastramqpi: FastRAMQPI,
    test_client: AsyncClient,
    trigger_sync,
    trigger_ldap_sync,
    add_ldap_person,
    dn2uuid,
) -> None:
    # Use unique person to avoid clashing with mo_person fixture
    cpr = "0101990002"
    person_dn_list = await add_ldap_person("perf2", cpr)
    person_dn = combine_dn_strings(person_dn_list)
    ldap_person_uuid = await dn2uuid(person_dn)
    
    # Ensure person is synced to MO first
    await trigger_ldap_sync(ldap_person_uuid)

    # Ensure app is started and context is populated
    ldap_connection = fastramqpi._context["user_context"]["ldap_connection"]
    graphql_client = fastramqpi._context["graphql_client"]

    # Find the MO employee
    mo_uuid = None
    async for attempt in retrying():
        with attempt:
            res = await graphql_client.read_employee_uuid_by_cpr_number(
                cpr_number=cpr
            )
            if res.objects:
                mo_uuid = one(res.objects).uuid
    
    assert mo_uuid is not None
    
    # We spy on the underlying ldap3 Connection.search method
    original_search = ldap_connection.search
    
    with patch.object(ldap_connection, 'search', side_effect=original_search) as mock_search:
        # Trigger sync from MO to LDAP
        await trigger_sync(mo_uuid)
        
        # We want to bring this down to 1 (batch search).
        assert mock_search.call_count == 1