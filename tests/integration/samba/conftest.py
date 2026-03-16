# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Fixture overrides for Samba AD integration tests.

The parent conftest (tests/conftest.py) provides fixtures targeting OpenLDAP.
This file overrides the fixtures that need Samba-specific behaviour, such as
connection credentials, purge logic (baseline-aware), and the OU layout.
"""

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import suppress

import pytest
from ldap3 import NO_ATTRIBUTES

from mo_ldap_import_export.config import Settings
from mo_ldap_import_export.ldap import configure_ldap_connection
from mo_ldap_import_export.ldap import connection_healthcheck
from mo_ldap_import_export.ldap import paged_search
from mo_ldap_import_export.ldapapi import LDAPAPI
from mo_ldap_import_export.types import DN

# Baseline DNs in a fresh Samba AD DC provisioned for MAGENTA.DK.
# Any DN found that is NOT in this set was created by a test.
SAMBA_BASELINE_DNS: set[DN] = {
    "CN=Account Operators,CN=Builtin,DC=magenta,DC=dk",
    "CN=AdminSDHolder,CN=System,DC=magenta,DC=dk",
    "CN=Administrator,CN=Users,DC=magenta,DC=dk",
    "CN=Administrators,CN=Builtin,DC=magenta,DC=dk",
    "CN=Allowed RODC Password Replication Group,CN=Users,DC=magenta,DC=dk",
    "CN=Backup Operators,CN=Builtin,DC=magenta,DC=dk",
    "CN=Builtin,DC=magenta,DC=dk",
    "CN=Cert Publishers,CN=Users,DC=magenta,DC=dk",
    "CN=Certificate Service DCOM Access,CN=Builtin,DC=magenta,DC=dk",
    "CN=ComPartitionSets,CN=System,DC=magenta,DC=dk",
    "CN=ComPartitions,CN=System,DC=magenta,DC=dk",
    "CN=Computers,DC=magenta,DC=dk",
    "CN=Cryptographic Operators,CN=Builtin,DC=magenta,DC=dk",
    "CN=DC,OU=Domain Controllers,DC=magenta,DC=dk",
    "CN=Denied RODC Password Replication Group,CN=Users,DC=magenta,DC=dk",
    "CN=Distributed COM Users,CN=Builtin,DC=magenta,DC=dk",
    "CN=DnsAdmins,CN=Users,DC=magenta,DC=dk",
    "CN=DnsUpdateProxy,CN=Users,DC=magenta,DC=dk",
    "CN=Domain Admins,CN=Users,DC=magenta,DC=dk",
    "CN=Domain Computers,CN=Users,DC=magenta,DC=dk",
    "CN=Domain Controllers,CN=Users,DC=magenta,DC=dk",
    "CN=Domain Guests,CN=Users,DC=magenta,DC=dk",
    "CN=Domain Users,CN=Users,DC=magenta,DC=dk",
    "CN=Enterprise Admins,CN=Users,DC=magenta,DC=dk",
    "CN=Enterprise Read-only Domain Controllers,CN=Users,DC=magenta,DC=dk",
    "CN=Event Log Readers,CN=Builtin,DC=magenta,DC=dk",
    "CN=File Replication Service,CN=System,DC=magenta,DC=dk",
    "CN=FileLinks,CN=System,DC=magenta,DC=dk",
    "CN=ForeignSecurityPrincipals,DC=magenta,DC=dk",
    "CN=Group Policy Creator Owners,CN=Users,DC=magenta,DC=dk",
    "CN=Guest,CN=Users,DC=magenta,DC=dk",
    "CN=Guests,CN=Builtin,DC=magenta,DC=dk",
    "CN=IIS_IUSRS,CN=Builtin,DC=magenta,DC=dk",
    "CN=Incoming Forest Trust Builders,CN=Builtin,DC=magenta,DC=dk",
    "CN=Infrastructure,DC=magenta,DC=dk",
    "CN=LostAndFound,DC=magenta,DC=dk",
    "CN=Machine,CN={31B2F340-016D-11D2-945F-00C04FB984F9},CN=Policies,CN=System,DC=magenta,DC=dk",
    "CN=Machine,CN={6AC1786C-016F-11D2-945F-00C04FB984F9},CN=Policies,CN=System,DC=magenta,DC=dk",
    "CN=NTDS Quotas,DC=magenta,DC=dk",
    "CN=Network Configuration Operators,CN=Builtin,DC=magenta,DC=dk",
    "CN=ObjectMoveTable,CN=FileLinks,CN=System,DC=magenta,DC=dk",
    "CN=Password Settings Container,CN=System,DC=magenta,DC=dk",
    "CN=Performance Log Users,CN=Builtin,DC=magenta,DC=dk",
    "CN=Performance Monitor Users,CN=Builtin,DC=magenta,DC=dk",
    "CN=Policies,CN=System,DC=magenta,DC=dk",
    "CN=Pre-Windows 2000 Compatible Access,CN=Builtin,DC=magenta,DC=dk",
    "CN=Print Operators,CN=Builtin,DC=magenta,DC=dk",
    "CN=Protected Users,CN=Users,DC=magenta,DC=dk",
    "CN=RAS and IAS Servers Access Check,CN=System,DC=magenta,DC=dk",
    "CN=RAS and IAS Servers,CN=Users,DC=magenta,DC=dk",
    "CN=RID Manager$,CN=System,DC=magenta,DC=dk",
    "CN=RID Set,CN=DC,OU=Domain Controllers,DC=magenta,DC=dk",
    "CN=Read-only Domain Controllers,CN=Users,DC=magenta,DC=dk",
    "CN=Remote Desktop Users,CN=Builtin,DC=magenta,DC=dk",
    "CN=Replicator,CN=Builtin,DC=magenta,DC=dk",
    "CN=RpcServices,CN=System,DC=magenta,DC=dk",
    "CN=S-1-5-11,CN=ForeignSecurityPrincipals,DC=magenta,DC=dk",
    "CN=S-1-5-17,CN=ForeignSecurityPrincipals,DC=magenta,DC=dk",
    "CN=S-1-5-4,CN=ForeignSecurityPrincipals,DC=magenta,DC=dk",
    "CN=S-1-5-9,CN=ForeignSecurityPrincipals,DC=magenta,DC=dk",
    "CN=Schema Admins,CN=Users,DC=magenta,DC=dk",
    "CN=Server Operators,CN=Builtin,DC=magenta,DC=dk",
    "CN=Server,CN=System,DC=magenta,DC=dk",
    "CN=System,DC=magenta,DC=dk",
    "CN=Terminal Server License Servers,CN=Builtin,DC=magenta,DC=dk",
    "CN=User,CN={31B2F340-016D-11D2-945F-00C04FB984F9},CN=Policies,CN=System,DC=magenta,DC=dk",
    "CN=User,CN={6AC1786C-016F-11D2-945F-00C04FB984F9},CN=Policies,CN=System,DC=magenta,DC=dk",
    "CN=Users,CN=Builtin,DC=magenta,DC=dk",
    "CN=Users,DC=magenta,DC=dk",
    "CN=VolumeTable,CN=FileLinks,CN=System,DC=magenta,DC=dk",
    "CN=Windows Authorization Access Group,CN=Builtin,DC=magenta,DC=dk",
    "CN=krbtgt,CN=Users,DC=magenta,DC=dk",
    "CN={31B2F340-016D-11D2-945F-00C04FB984F9},CN=Policies,CN=System,DC=magenta,DC=dk",
    "CN={6AC1786C-016F-11D2-945F-00C04FB984F9},CN=Policies,CN=System,DC=magenta,DC=dk",
    "OU=Domain Controllers,DC=magenta,DC=dk",
}


@pytest.fixture
def integration_test_environment_variables(
    integration_test_environment_variables: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Override: layer Samba AD envvars on top of the parent's OpenLDAP defaults."""
    samba_envvars = {
        "LDAP_CONTROLLERS": json.dumps([{"host": "samba", "port": 389}]),
        "LDAP_USER": "CN=Administrator,CN=Users,DC=magenta,DC=dk",
        "LDAP_PASSWORD": "AdminPassword123",
        "LDAP_SEARCH_BASE": "DC=magenta,DC=dk",
        "LDAP_OUS_TO_SEARCH_IN": json.dumps(["CN=Users"]),
        "LDAP_OU_FOR_NEW_USERS": "CN=Users",
        "LDAP_DIALECT": "AD",
    }
    for key, value in samba_envvars.items():
        monkeypatch.setenv(key, value)


@pytest.fixture(autouse=True)
async def wait_for_samba(integration_test_environment_variables: None) -> None:
    """Wait for Samba AD DC to finish provisioning."""
    async with asyncio.timeout(120):
        while True:
            with suppress(Exception):
                connection = configure_ldap_connection(Settings())
                if await connection_healthcheck(connection):
                    return
            await asyncio.sleep(1)


@pytest.fixture
async def purge_ldap(write_ldap_api: LDAPAPI) -> AsyncIterator[None]:
    """Override purge_ldap for Samba AD.

    Uses a baseline of all DNs in a freshly provisioned Samba AD DC.
    After the test, any DN not in the baseline is deleted.
    """
    settings = Settings()

    async def find_all_dns() -> set[DN]:
        response = await paged_search(
            settings,
            write_ldap_api.ldap_connection.connection,
            {"search_filter": "(objectclass=*)", "attributes": NO_ATTRIBUTES},
            search_base=settings.ldap_search_base,
        )
        dns = {entry["dn"] for entry in response}
        dns.discard(settings.ldap_search_base)
        return dns

    async def dns_to_delete() -> set[DN]:
        """Find DNs created by tests (not part of the Samba baseline)."""
        return (await find_all_dns()) - SAMBA_BASELINE_DNS

    yield

    while dns := await dns_to_delete():
        for dn in dns:
            with suppress(Exception):
                await write_ldap_api.ldap_connection.ldap_delete(dn)


@pytest.fixture
def ldap_suffix() -> list[str]:
    """Override: Samba returns uppercase DC= in DNs."""
    return ["DC=magenta", "DC=dk"]


@pytest.fixture
def ldap_org_unit(ldap_suffix: list[str]) -> list[str]:
    """Override: CN=Users already exists in Samba AD, skip ldap_org creation."""
    return ["CN=Users"] + ldap_suffix
