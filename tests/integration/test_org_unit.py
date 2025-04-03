# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json
from unittest.mock import ANY

import pytest
from fastramqpi.pytest_util import retry
from ldap3 import Connection
from more_itertools import one

from mo_ldap_import_export.autogenerated_graphql_client import GraphQLClient
from mo_ldap_import_export.ldap import ldap_add
from mo_ldap_import_export.ldap import ldap_modify
from mo_ldap_import_export.ldap import ldap_modify_dn
from mo_ldap_import_export.utils import combine_dn_strings
from mo_ldap_import_export.utils import mo_today
from tests.integration.conftest import DN2UUID


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "True",
        "CONVERSION_MAPPING": json.dumps(
            {
                "ldap_to_mo_org_unit": {
                    "OrganisationUnit": {
                        "objectClass": "OrganisationUnit",
                        "_import_to_mo_": "true",
                        "_ldap_attributes_": ["entryUUID", "ou", "l"],
                        # "l" is arbitrarily chosen as an enabled/disabled marker
                        "_terminate_": "{{ now()|mo_datestring if ldap.l == 'EXPIRED' else '' }}",
                        "uuid": "{{ get_org_unit_uuid({'user_keys': [ldap.entryUUID]}) or uuid4() }}",  # TODO: why is this required?
                        "user_key": "{{ ldap.entryUUID }}",
                        "name": "{{ ldap.ou }}",
                        "unit_type": "{{ get_org_unit_type_uuid('Afdeling') }}",
                    },
                },
                # TODO: why is this required?
                "username_generator": {
                    "combinations_to_try": ["FFFX", "LLLX"],
                },
            }
        ),
    }
)
@pytest.mark.usefixtures("test_client")
async def test_to_mo(
    graphql_client: GraphQLClient,
    ldap_connection: Connection,
    ldap_org: list[str],
    dn2uuid: DN2UUID,
) -> None:
    @retry()
    async def assert_org_unit(expected: dict) -> None:
        org_units = await graphql_client._testing__org_unit_read()
        org_unit = one(org_units.objects)
        validities = one(org_unit.validities)
        assert validities.dict() == expected

    org_unit_dn = combine_dn_strings(["ou=create"] + ldap_org)

    # LDAP: Create
    await ldap_add(
        ldap_connection,
        dn=org_unit_dn,
        object_class=["top", "organizationalUnit"],
        attributes={
            "objectClass": ["top", "organizationalUnit"],
        },
    )
    ldap_org_unit_uuid = await dn2uuid(org_unit_dn)

    mo_org_unit = {
        "uuid": ANY,
        "user_key": str(ldap_org_unit_uuid),
        "name": "create",
        "unit_type": {"user_key": "Afdeling"},
        "validity": {
            "from_": mo_today(),
            "to": None,
        },
    }
    await assert_org_unit(mo_org_unit)

    # LDAP: Edit
    await ldap_modify_dn(
        ldap_connection,
        dn=org_unit_dn,
        relative_dn="ou=edit",
    )
    mo_org_unit = {
        **mo_org_unit,
        "name": "edit",
    }
    await assert_org_unit(mo_org_unit)

    org_unit_dn = combine_dn_strings(["ou=edit"] + ldap_org)

    # LDAP: Terminate
    await ldap_modify(
        ldap_connection,
        dn=org_unit_dn,
        changes={
            "l": [("MODIFY_REPLACE", "EXPIRED")],
        },
    )
    mo_org_unit = {
        **mo_org_unit,
        "validity": {"from_": mo_today(), "to": mo_today()},
    }
    await assert_org_unit(mo_org_unit)
