# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Integration test for the gssapi LDAP auth backend against Samba AD DC."""

import json

import pytest
from ldap3 import GSSAPI
from ldap3 import SASL

from mo_ldap_import_export.ldapapi import LDAPAPI
from mo_ldap_import_export.utils import combine_dn_strings

from .conftest import SAMBA_HOST


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LDAP_CONTROLLERS": json.dumps([{"host": SAMBA_HOST, "port": 389}]),
        "LDAP_AUTH_METHOD": "gssapi",
    }
)
async def test_gssapi_bind_and_read_person(
    ldap_api: LDAPAPI, ldap_org_unit: list[str]
) -> None:
    """Bind to Samba AD with Kerberos and create/read a user over that connection."""
    connection = ldap_api.ldap_connection.connection
    assert connection.authentication == SASL
    assert connection.sasl_mechanism == GSSAPI
    # AD reports the identity behind the Kerberos ticket, not a bind DN
    assert connection.extend.standard.who_am_i() == "u:MAGENTA\\Administrator"

    person_dn = combine_dn_strings(["CN=Kerberos Test"] + ldap_org_unit)
    await ldap_api.add_ldap_object(
        person_dn,
        object_class="user",
        attributes={
            "sn": ["Test"],
            "givenName": ["Kerberos"],
            "sAMAccountName": ["krbtest"],
            "userPrincipalName": ["krbtest@magenta.dk"],
        },
    )

    result = await ldap_api.get_object_by_dn(person_dn, {"cn", "sn"})
    assert hasattr(result, "cn")
    assert hasattr(result, "sn")
    assert result.cn == "Kerberos Test"
    assert result.sn == "Test"
