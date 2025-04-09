# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json
from collections.abc import Awaitable
from collections.abc import Callable
from unittest.mock import ANY

import pytest
from fastramqpi.pytest_util import retry
from ldap3 import Connection
from more_itertools import one
from more_itertools import only

from mo_ldap_import_export.autogenerated_graphql_client import GraphQLClient
from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    OrganisationUnitFilter,
)
from mo_ldap_import_export.ldap import ldap_add
from mo_ldap_import_export.ldap import ldap_modify
from mo_ldap_import_export.ldap import ldap_modify_dn
from mo_ldap_import_export.types import LDAPUUID
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
                "ldap_to_mo_any": {
                    "organizationalUnit": {
                        "OrganisationUnit": {
                            "objectClass": "OrganisationUnit",
                            "_import_to_mo_": "true",
                            "_ldap_attributes_": ["entryUUID", "ou", "l"],
                            # "l" is arbitrarily chosen as an enabled/disabled marker
                            "_terminate_": "{{ now()|mo_datestring if ldap.l == 'EXPIRED' else '' }}",
                            "uuid": "{{ get_org_unit_uuid({'user_keys': [ldap.entryUUID]}) or uuid4() }}",
                            "user_key": "{{ ldap.entryUUID }}",
                            "name": "{{ ldap.ou }}",
                            "unit_type": "{{ get_org_unit_type_uuid('Afdeling') }}",
                        },
                    }
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
        "parent": None,
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


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": json.dumps(
            {
                "ldap_to_mo_any": {
                    "organizationalUnit": {
                        "OrganisationUnit": {
                            "objectClass": "OrganisationUnit",
                            "_import_to_mo_": "true",
                            "_ldap_attributes_": ["entryUUID", "ou"],
                            "uuid": "{{ get_org_unit_uuid({'user_keys': [ldap.entryUUID]}) or uuid4() }}",
                            "user_key": "{{ ldap.entryUUID }}",
                            "name": "{{ ldap.ou }}",
                            "parent": """
                                {% set parent_dn = parent_dn(ldap.dn) %}
                                {% if dn_has_ou(parent_dn) %}
                                    {{ skip_if_none(get_org_unit_uuid({'user_keys': [dn_to_uuid(parent_dn)|string]})) }}
                                {% endif %}
                            """,
                            "unit_type": "{{ get_org_unit_type_uuid('Afdeling') }}",
                        },
                    }
                },
                # TODO: why is this required?
                "username_generator": {
                    "combinations_to_try": ["FFFX", "LLLX"],
                },
            }
        ),
    }
)
async def test_to_mo_parent(
    graphql_client: GraphQLClient,
    ldap_connection: Connection,
    ldap_org_unit: list[str],
    ldap_org_unit_uuid: LDAPUUID,
    trigger_ldap_sync: Callable[[LDAPUUID], Awaitable[None]],
    dn2uuid: DN2UUID,
) -> None:
    async def get_mo_org_unit(user_key: str) -> dict | None:
        org_units = await graphql_client._testing__org_unit_read(
            filter=OrganisationUnitFilter(user_keys=[user_key])
        )
        org_unit = only(org_units.objects)
        if org_unit is None:
            return None
        validities = one(org_unit.validities)
        return validities.dict()

    # Synchronise parent
    assert await get_mo_org_unit(str(ldap_org_unit_uuid)) is None
    await trigger_ldap_sync(ldap_org_unit_uuid)
    mo_parent = await get_mo_org_unit(str(ldap_org_unit_uuid))
    assert mo_parent is not None
    assert mo_parent == {
        "uuid": ANY,
        "user_key": str(ldap_org_unit_uuid),
        "name": "os2mo",
        "parent": None,
        "unit_type": {"user_key": "Afdeling"},
        "validity": {
            "from_": mo_today(),
            "to": None,
        },
    }

    # Add child to ldap
    child_unit_dn = combine_dn_strings(["ou=child"] + ldap_org_unit)
    await ldap_add(
        ldap_connection,
        dn=child_unit_dn,
        object_class=["top", "organizationalUnit"],
        attributes={
            "objectClass": ["top", "organizationalUnit"],
        },
    )
    ldap_child_org_unit_uuid = await dn2uuid(child_unit_dn)

    # Synchronise child
    assert await get_mo_org_unit(str(ldap_child_org_unit_uuid)) is None
    await trigger_ldap_sync(ldap_child_org_unit_uuid)
    mo_child = await get_mo_org_unit(str(ldap_child_org_unit_uuid))
    assert mo_child is not None
    assert mo_child == {
        "uuid": ANY,
        "user_key": str(ldap_child_org_unit_uuid),
        "name": "child",
        "parent": {"uuid": mo_parent["uuid"]},
        "unit_type": {"user_key": "Afdeling"},
        "validity": {
            "from_": mo_today(),
            "to": None,
        },
    }

    # Add grandchild to ldap
    grandchild_unit_dn = combine_dn_strings(
        ["ou=grandchild", "ou=child"] + ldap_org_unit
    )
    await ldap_add(
        ldap_connection,
        dn=grandchild_unit_dn,
        object_class=["top", "organizationalUnit"],
        attributes={
            "objectClass": ["top", "organizationalUnit"],
        },
    )
    ldap_grandchild_org_unit_uuid = await dn2uuid(grandchild_unit_dn)

    # Synchronise grandchild
    assert await get_mo_org_unit(str(ldap_grandchild_org_unit_uuid)) is None
    await trigger_ldap_sync(ldap_grandchild_org_unit_uuid)
    assert await get_mo_org_unit(str(ldap_grandchild_org_unit_uuid)) == {
        "uuid": ANY,
        "user_key": str(ldap_grandchild_org_unit_uuid),
        "name": "grandchild",
        "parent": {"uuid": mo_child["uuid"]},
        "unit_type": {"user_key": "Afdeling"},
        "validity": {
            "from_": mo_today(),
            "to": None,
        },
    }

    # Add new organisational unit to ldap WITHOUT adding to mo
    delayed_unit_dn = combine_dn_strings(["ou=delayed"] + ldap_org_unit)
    await ldap_add(
        ldap_connection,
        dn=delayed_unit_dn,
        object_class=["top", "organizationalUnit"],
        attributes={
            "objectClass": ["top", "organizationalUnit"],
        },
    )
    ldap_delayed_org_unit_uuid = await dn2uuid(delayed_unit_dn)

    # Add child of delayed to ldap
    mcfly_unit_dn = combine_dn_strings(["ou=mcfly", "ou=delayed"] + ldap_org_unit)
    await ldap_add(
        ldap_connection,
        dn=mcfly_unit_dn,
        object_class=["top", "organizationalUnit"],
        attributes={
            "objectClass": ["top", "organizationalUnit"],
        },
    )
    ldap_mcfly_org_unit_uuid = await dn2uuid(mcfly_unit_dn)

    # Synchronisation is skipped because parent is not in mo yet
    assert await get_mo_org_unit(str(ldap_mcfly_org_unit_uuid)) is None
    await trigger_ldap_sync(ldap_mcfly_org_unit_uuid)
    assert await get_mo_org_unit(str(ldap_mcfly_org_unit_uuid)) is None

    # Synchronise delayed + mcfly
    await trigger_ldap_sync(ldap_delayed_org_unit_uuid)
    await trigger_ldap_sync(ldap_mcfly_org_unit_uuid)
    assert await get_mo_org_unit(str(ldap_mcfly_org_unit_uuid)) == {
        "uuid": ANY,
        "user_key": str(ldap_mcfly_org_unit_uuid),
        "name": "mcfly",
        "parent": {"uuid": ANY},
        "unit_type": {"user_key": "Afdeling"},
        "validity": {
            "from_": mo_today(),
            "to": None,
        },
    }
