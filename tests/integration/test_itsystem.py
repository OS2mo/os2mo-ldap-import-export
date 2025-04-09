# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json
from collections.abc import Awaitable
from collections.abc import Callable
from uuid import UUID

import pytest
from ldap3 import Connection

from mo_ldap_import_export.ldap import ldap_add
from mo_ldap_import_export.types import DN
from mo_ldap_import_export.types import LDAPUUID
from mo_ldap_import_export.utils import combine_dn_strings
from tests.integration.conftest import DN2UUID


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": json.dumps(
            {
                "ldap_to_mo_object_class": {
                    # GroupOfNames is the objectClass we listen to
                    "groupOfNames": {
                        "ITSystem": {
                            "objectClass": "ITSystem",
                            "_import_to_mo_": "true",
                            "_ldap_attributes_": ["cn", "entryUUID"],
                            "uuid": "{{ get_itsystem_uuid({'user_keys': [ldap.entryUUID]}) }}",
                            "user_key": "{{ ldap.entryUUID }}",
                            "name": "{{ ldap.cn }}",
                        }
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
@pytest.mark.xfail(reason="Cannot create ITSystems")
async def test_to_mo(
    trigger_ldap_sync: Callable[[LDAPUUID], Awaitable[None]],
    dn2uuid: DN2UUID,
    ldap_connection: Connection,
    ldap_org: list[str],
    ldap_person_dn: DN,
    read_itsystem_by_user_key: Callable[[str], Awaitable[UUID]],
) -> None:
    # Create an LDAP group with one member
    group_dn = combine_dn_strings(["cn=os2mo"] + ldap_org)
    await ldap_add(
        ldap_connection,
        dn=group_dn,
        object_class=["top", "GroupOfNames"],
        attributes={
            "objectClass": ["top", "GroupOfNames"],
            "member": [ldap_person_dn],
        },
    )
    group_uuid = await dn2uuid(group_dn)

    # Trigger synchronization
    await trigger_ldap_sync(group_uuid)

    # Check that an ITSystem was created from our LDAP group
    await read_itsystem_by_user_key(str(group_uuid))
