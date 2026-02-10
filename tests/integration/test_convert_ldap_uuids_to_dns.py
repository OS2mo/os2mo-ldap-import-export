# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import cast
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from mo_ldap_import_export.ldapapi import LDAPAPI
from mo_ldap_import_export.types import LDAPUUID


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
async def test_convert_ldap_uuids_to_dns(
    ldap_api: LDAPAPI,
    ldap_person_uuid: LDAPUUID,
) -> None:
    missing_uuid = cast(LDAPUUID, uuid4())

    # Convert empty list
    result = await ldap_api.convert_ldap_uuids_to_dns(
        ldap_uuids=set(), attributes=set()
    )
    assert result == {}

    # Convert missing LDAP UUID
    result = await ldap_api.convert_ldap_uuids_to_dns(
        ldap_uuids={missing_uuid}, attributes=set()
    )
    assert result == {missing_uuid: None}

    # Convert existing LDAP UUID
    result = await ldap_api.convert_ldap_uuids_to_dns(
        ldap_uuids={ldap_person_uuid}, attributes=set()
    )
    assert len(result) == 1
    obj = result[ldap_person_uuid]
    assert obj is not None
    assert obj.dn == "uid=abk,ou=os2mo,o=magenta,dc=magenta,dc=dk"

    # Convert existing and non-existing LDAP UUIDs
    result = await ldap_api.convert_ldap_uuids_to_dns(
        ldap_uuids={ldap_person_uuid, missing_uuid}, attributes=set()
    )
    assert len(result) == 2
    assert result[missing_uuid] is None
    obj = result[ldap_person_uuid]
    assert obj is not None
    assert obj.dn == "uid=abk,ou=os2mo,o=magenta,dc=magenta,dc=dk"

    # Convert existing UUID, but LDAP is down
    exception = ValueError("BOOM")
    ldap_api.ldap_connection.connection = MagicMock()
    ldap_api.ldap_connection.connection.search.side_effect = exception

    with pytest.raises(ValueError) as exc_info:
        await ldap_api.convert_ldap_uuids_to_dns(
            ldap_uuids={ldap_person_uuid}, attributes=set()
        )

    assert "Exceptions during UUID2DN translation" in str(exc_info.value)
    assert exc_info.value.__cause__ is exception

