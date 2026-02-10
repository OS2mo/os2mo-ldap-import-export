# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastramqpi.context import Context

from mo_ldap_import_export.customer_specific_checks import ExportChecks
from mo_ldap_import_export.exceptions import IncorrectMapping
from mo_ldap_import_export.import_export import SyncTool
from mo_ldap_import_export.ldap_classes import LdapObject


@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": json.dumps(
            {
                "ldap_to_mo": {
                    "Active Directory": {
                        "objectClass": "ITUser",
                        "_import_to_mo_": "True",
                        "_ldap_attributes_": ["msSFU30Name"],
                        "uuid": "{{ employee_uuid }}",
                        "user_key": "{{ ldap.msSFU30Name or '' }}",
                        "itsystem": "{ 'hep': 'hey }",  # Malformed JSON
                        "person": "{{ dict(uuid=employee_uuid or '') }}",
                    }
                },
            }
        ),
    }
)
@pytest.mark.integration_test
async def test_ldap_to_mo_dict_error(context: Context) -> None:
    """Test for IncorrectMapping when a Jinja template produces malformed JSON."""
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
    settings = user_context["settings"]

    with pytest.raises(IncorrectMapping) as exc_info:
        assert settings.conversion_mapping.ldap_to_mo is not None
        await sync_tool.import_single_entity(
            mapping=settings.conversion_mapping.ldap_to_mo["Active Directory"],
            ldap_object=LdapObject(
                dn="",
                msSFU30Name=["bar"],
                itSystemName=["Active Directory"],
            ),
            template_context={
                "employee_uuid": str(uuid4()),
            },
            dry_run=False,
        )
        assert "Could not convert { 'hep': 'hey } in 'itsystem' to dict" in str(
            exc_info.value
        )
        # Check that context contains some expected keys, since order might vary
        error_msg = str(exc_info.value)
        assert "msSFU30Name" in error_msg
        assert "itSystemName" in error_msg
        assert "'dn': ''" in error_msg
