# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Shared fixtures for Samba AD integration tests."""

import json
import time
from collections.abc import AsyncIterator
from contextlib import suppress

import ldap3
import pytest
from fastramqpi.main import FastRAMQPI
from ldap3 import NO_ATTRIBUTES

from mo_ldap_import_export.config import Settings
from mo_ldap_import_export.ldap import _paged_search
from mo_ldap_import_export.ldap import configure_ldap_connection
from mo_ldap_import_export.ldapapi import LDAPAPI
from mo_ldap_import_export.main import create_fastramqpi
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
    "CN=Protected Users,CN=Users,DC=magenta,DC=dk",
    "CN=RAS and IAS Servers,CN=Users,DC=magenta,DC=dk",
    "CN=Read-only Domain Controllers,CN=Users,DC=magenta,DC=dk",
    "CN=Schema Admins,CN=Users,DC=magenta,DC=dk",
}


# Settings constructor kwargs for Samba — these use init_settings priority which
# overrides both /var/run/config.yaml and environment variables.
SAMBA_SETTINGS_KWARGS = {
    "ldap_controllers": [{"host": "samba", "port": 389}],
    "ldap_domain": SAMBA_ENVVARS["LDAP_DOMAIN"],
    "ldap_user": SAMBA_ENVVARS["LDAP_USER"],
    "ldap_password": SAMBA_ENVVARS["LDAP_PASSWORD"],
    "ldap_search_base": SAMBA_ENVVARS["LDAP_SEARCH_BASE"],
    "ldap_ous_to_search_in": ["CN=Users"],
    "ldap_ou_for_new_users": SAMBA_ENVVARS["LDAP_OU_FOR_NEW_USERS"],
    "ldap_dialect": "AD",
    "ldap_auth_method": "simple",
}


@pytest.fixture(autouse=True, scope="session")
def wait_for_samba() -> None:
    """Wait for Samba AD DC to finish provisioning before running tests.

    The smblds container needs time to run ``samba-tool domain provision``
    after starting. The LDAP port may accept connections before provisioning
    completes, but authenticated binds will fail with ``invalidCredentials``
    until the domain is fully set up.
    """
    server = ldap3.Server("samba", port=389, get_info=ldap3.NONE)
    last_exc: Exception | None = None
    for _ in range(60):
        try:
            conn = ldap3.Connection(
                server,
                user=SAMBA_ENVVARS["LDAP_USER"],
                password=SAMBA_ENVVARS["LDAP_PASSWORD"],
                authentication=ldap3.SIMPLE,
                auto_bind=True,
                raise_exceptions=True,
            )
            conn.unbind()
            return
        except Exception as exc:
            last_exc = exc
            time.sleep(2)
    pytest.fail(f"Samba AD DC not ready after 120s: {last_exc}")


@pytest.fixture
async def write_ldap_api(load_marked_envvars: None) -> LDAPAPI:
    """Override to point to Samba AD DC instead of OpenLDAP.

    The Samba connection settings are passed as constructor kwargs so they take
    the highest priority in Pydantic's settings source chain — overriding both
    ``/var/run/config.yaml`` (created by the CI template with OpenLDAP
    credentials) and environment variables.
    """
    settings = Settings(**SAMBA_SETTINGS_KWARGS)
    return LDAPAPI(settings, configure_ldap_connection(settings))


@pytest.fixture
async def fastramqpi(load_marked_envvars: None) -> FastRAMQPI:
    """Override to pass Samba settings as init kwargs (highest priority)."""
    return create_fastramqpi(**SAMBA_SETTINGS_KWARGS)


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
