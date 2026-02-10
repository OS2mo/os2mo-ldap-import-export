# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json
from unittest.mock import ANY
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastramqpi.context import Context

from mo_ldap_import_export.customer_specific_checks import ExportChecks
from mo_ldap_import_export.import_export import SyncTool
from mo_ldap_import_export.ldap_classes import LdapObject


@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": json.dumps(
            {
                "ldap_to_mo": {
                    "Employee": {
                        "objectClass": "Employee",
                        "_import_to_mo_": "True",
                        "_ldap_attributes_": ["givenName", "sn"],
                        "user_key": "{{ ldap.dn }}",
                        "given_name": "{{ ldap.get('givenName', 'given_name') }}",
                        "surname": "{{ ldap.sn if 'sn' in ldap else 'surname' }}",
                        "cpr_number": "{{ ldap.get('cpr') }}",
                        "uuid": "{{ employee_uuid }}",
                    }
                }
            }
        ),
    }
)
@pytest.mark.integration_test
@pytest.mark.parametrize(
    "ldap_values,expected",
    (
        # Base case
        ({}, {}),
        # Single overrides
        ({"cpr": "0101700000"}, {"cpr_number": "0101700000"}),
        ({"givenName": "Hans"}, {"given_name": "Hans"}),
        ({"sn": "Petersen"}, {"surname": "Petersen"}),
        # Empty values -> no keys
        ({"cpr": ""}, {}),
        ({"givenName": ""}, {"given_name": None}),
        ({"sn": ""}, {"surname": None}),
    ),
)
async def test_template_strictness(
    context: Context, ldap_values: dict[str, str], expected: dict[str, str]
) -> None:
    """Test various Jinja template strictness scenarios in LdapConverter."""
    user_context = context["user_context"]
    dataloader = user_context["dataloader"]
    sync_tool = SyncTool(
        dataloader=dataloader,
        converter=user_context["converter"],
        export_checks=ExportChecks(dataloader),
        settings=user_context["settings"],
        ldap_connection=user_context["ldap_connection"],
    )
    # Mock moapi to avoid side-effects
    sync_tool.dataloader.moapi = AsyncMock()
    # Mock fetch_uuid_object to return None so we always get Verb.CREATE and don't try to subscript it
    sync_tool.fetch_uuid_object = AsyncMock(return_value=None)  # type: ignore[method-assign]

    settings = user_context["settings"]

    assert settings.conversion_mapping.ldap_to_mo is not None
    # Ensure required attributes are present to satisfy import_single_entity's assertion
    full_ldap_values = {"givenName": "given_name", "sn": "surname", **ldap_values}
    await sync_tool.import_single_entity(
        mapping=settings.conversion_mapping.ldap_to_mo["Employee"],
        ldap_object=LdapObject(dn="CN=foo", **full_ldap_values),
        template_context={
            "employee_uuid": str(uuid4()),
        },
        dry_run=False,
    )

    # Get the captured object
    assert sync_tool.dataloader.moapi.create.called
    employee = sync_tool.dataloader.moapi.create.call_args[0][0]

    expected_employee = {
        "uuid": ANY,
        "user_key": "CN=foo",
        "given_name": "given_name",
        "surname": "surname",
    }
    for key, value in expected.items():
        if value is None:
            del expected_employee[key]
        else:
            expected_employee[key] = value

    assert employee.dict(exclude_unset=True) == expected_employee
