# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Integration tests for Samba AD DC."""

import pytest

from mo_ldap_import_export.ldapapi import LDAPAPI
from mo_ldap_import_export.utils import combine_dn_strings


@pytest.mark.integration_test
@pytest.mark.usefixtures("test_client")
async def test_samba_create_and_read_person(
    ldap_api: LDAPAPI,
    ldap_org_unit: list[str],
) -> None:
    """Test creating a person in Samba AD and reading it back."""
    person_dn = combine_dn_strings(["CN=John Doe"] + ldap_org_unit)
    await ldap_api.add_ldap_object(
        person_dn,
        object_class="user",
        attributes={
            "sn": ["Doe"],
            "givenName": ["John"],
            "sAMAccountName": ["jdoe"],
            "userPrincipalName": ["jdoe@magenta.dk"],
        },
    )

    result = await ldap_api.get_object_by_dn(person_dn, {"cn", "sn"})
    assert hasattr(result, "cn")
    assert hasattr(result, "sn")
    assert result.cn == "John Doe"
    assert result.sn == "Doe"
