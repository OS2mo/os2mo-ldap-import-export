# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Shared fixtures for Samba AD integration tests."""

import json
from collections.abc import AsyncIterator
from contextlib import suppress

import pytest
from ldap3 import NO_ATTRIBUTES

from mo_ldap_import_export.config import Settings
from mo_ldap_import_export.ldap import _paged_search
from mo_ldap_import_export.ldap import configure_ldap_connection
from mo_ldap_import_export.ldapapi import LDAPAPI
from mo_ldap_import_export.types import DN

SAMBA_ENVVARS = {
    "LDAP_CONTROLLERS": json.dumps([{"host": "samba", "port": 389}]),
    "LDAP_DOMAIN": "magenta.dk",
    "LDAP_USER": "CN=Administrator,CN=Users,DC=magenta,DC=dk",
    "LDAP_PASSWORD": "AdminPassword123!",
    "LDAP_SEARCH_BASE": "DC=magenta,DC=dk",
    "LDAP_OUS_TO_SEARCH_IN": json.dumps(["CN=Users"]),
    "LDAP_OU_FOR_NEW_USERS": "CN=Users",
    "LDAP_DIALECT": "AD",
    "LDAP_AUTH_METHOD": "simple",
}

# Baseline DNs under CN=Users in a fresh Samba AD DC provisioned for MAGENTA.DK.
# Any DN found under CN=Users that is NOT in this set was created by a test.
SAMBA_BASELINE_USER_DNS: set[DN] = {
    "CN=Administrator,CN=Users,DC=magenta,DC=dk",
    "CN=Allowed RODC Password Replication Group,CN=Users,DC=magenta,DC=dk",
    "CN=Cert Publishers,CN=Users,DC=magenta,DC=dk",
    "CN=Denied RODC Password Replication Group,CN=Users,DC=magenta,DC=dk",
    "CN=DnsAdmins,CN=Users,DC=magenta,DC=dk",
    "CN=DnsUpdateProxy,CN=Users,DC=magenta,DC=dk",
    "CN=Domain Admins,CN=Users,DC=magenta,DC=dk",
    "CN=Domain Computers,CN=Users,DC=magenta,DC=dk",
    "CN=Domain Controllers,CN=Users,DC=magenta,DC=dk",
    "CN=Domain Guests,CN=Users,DC=magenta,DC=dk",
    "CN=Domain Users,CN=Users,DC=magenta,DC=dk",
    "CN=Enterprise Admins,CN=Users,DC=magenta,DC=dk",
    "CN=Enterprise Read-only Domain Controllers,CN=Users,DC=magenta,DC=dk",
    "CN=Group Policy Creator Owners,CN=Users,DC=magenta,DC=dk",
    "CN=Guest,CN=Users,DC=magenta,DC=dk",
    "CN=krbtgt,CN=Users,DC=magenta,DC=dk",
    "CN=RAS and IAS Servers,CN=Users,DC=magenta,DC=dk",
    "CN=Read-only Domain Controllers,CN=Users,DC=magenta,DC=dk",
    "CN=Schema Admins,CN=Users,DC=magenta,DC=dk",
}


@pytest.fixture
async def write_ldap_api(load_marked_envvars: None) -> LDAPAPI:
    """Override to point to Samba AD DC instead of OpenLDAP."""
    settings = Settings()
    return LDAPAPI(settings, configure_ldap_connection(settings))


@pytest.fixture
async def purge_ldap(write_ldap_api: LDAPAPI) -> AsyncIterator[None]:
    """Override purge_ldap for Samba AD.

    Uses an explicit baseline of known CN=Users DNs in a fresh Samba AD DC.
    After the test, any DN found under CN=Users that is not in the baseline
    is deleted.
    """
    users_base = "CN=Users,DC=magenta,DC=dk"

    async def find_user_dns() -> set[DN]:
        response = await _paged_search(
            write_ldap_api.ldap_connection.connection,
            {"search_filter": "(objectclass=*)", "attributes": NO_ATTRIBUTES},
            users_base,
        )
        return {
            entry["dn"]
            for entry in response
            if entry["type"] == "searchResEntry" and entry.get("dn") != users_base
        }

    yield

    # Delete entries created during the test
    while new_dns := (await find_user_dns()) - SAMBA_BASELINE_USER_DNS:
        for dn in new_dns:
            with suppress(Exception):
                await write_ldap_api.ldap_connection.ldap_delete(dn)
        if (await find_user_dns()) - SAMBA_BASELINE_USER_DNS == new_dns:
            break  # Can't delete any more


@pytest.fixture
def ldap_org_unit(ldap_suffix: list[str]) -> list[str]:
    """Override: CN=Users already exists in Samba AD, skip ldap_org creation."""
    return ["CN=Users"] + ldap_suffix
