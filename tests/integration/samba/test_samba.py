# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Integration tests for Samba AD DC (Active Directory compatible LDAP server)."""
import json
from collections.abc import AsyncIterator

import ldap3
import pytest
from fastramqpi.context import Context

from mo_ldap_import_export.config import Settings
from mo_ldap_import_export.ldap import configure_ldap_connection
from mo_ldap_import_export.ldapapi import LDAPAPI
from mo_ldap_import_export.utils import combine_dn_strings

SAMBA_HOST = "samba"
SAMBA_PORT = 389

SAMBA_ENVVARS = {
    "LDAP_CONTROLLERS": json.dumps([{"host": SAMBA_HOST, "port": SAMBA_PORT}]),
    "LDAP_DOMAIN": "magenta.dk",
    "LDAP_USER": "CN=Administrator,CN=Users,DC=magenta,DC=dk",
    "LDAP_PASSWORD": "AdminPassword123!",
    "LDAP_SEARCH_BASE": "DC=magenta,DC=dk",
    "LDAP_OUS_TO_SEARCH_IN": json.dumps(["CN=Users"]),
    "LDAP_OU_FOR_NEW_USERS": "CN=Users",
    "LDAP_DIALECT": "AD",
    "LDAP_AUTH_METHOD": "simple",
}


@pytest.fixture
async def write_ldap_api(load_marked_envvars: None) -> LDAPAPI:
    """Override to point to Samba AD DC instead of OpenLDAP."""
    settings = Settings()
    return LDAPAPI(settings, configure_ldap_connection(settings))


@pytest.fixture
async def purge_ldap() -> AsyncIterator[None]:
    """Override purge_ldap to be a no-op for Samba AD.

    Samba AD has system objects (CN=Users, OU=Domain Controllers, etc.)
    that cannot be deleted. Tests clean up after themselves via try/finally.
    """
    yield


@pytest.fixture
def ldap_suffix() -> list[str]:
    """Override ldap_suffix for Samba AD."""
    return ["DC=magenta", "DC=dk"]


@pytest.fixture
def ldap_org_unit(ldap_suffix: list[str]) -> list[str]:
    """Use CN=Users which already exists in Samba AD."""
    return ["CN=Users"] + ldap_suffix


@pytest.mark.integration_test
@pytest.mark.envvar(SAMBA_ENVVARS)
@pytest.mark.usefixtures("test_client")
async def test_samba_create_and_read_persons(
    context: Context,
    ldap_org_unit: list[str],
) -> None:
    """Test creating persons in Samba AD and reading them back."""
    ldap_connection = context["user_context"]["dataloader"].ldapapi.ldap_connection

    persons = [
        {
            "cn": "John Doe",
            "sn": "Doe",
            "givenName": "John",
            "sAMAccountName": "jdoe",
            "userPrincipalName": "jdoe@magenta.dk",
        },
        {
            "cn": "Alice Smith",
            "sn": "Smith",
            "givenName": "Alice",
            "sAMAccountName": "asmith",
            "userPrincipalName": "asmith@magenta.dk",
        },
    ]

    created_dns: list[list[str]] = []
    try:
        for person in persons:
            person_dn = [f"CN={person['cn']}"] + ldap_org_unit
            await ldap_connection.ldap_add(
                combine_dn_strings(person_dn),
                object_class=["top", "person", "organizationalPerson", "user"],
                attributes={
                    "objectClass": ["top", "person", "organizationalPerson", "user"],
                    **person,
                },
            )
            created_dns.append(person_dn)

        # Read back and verify
        for person in persons:
            search_base = combine_dn_strings(ldap_org_unit)
            response, result = await ldap_connection.ldap_search(
                search_base=search_base,
                search_filter=f"(sAMAccountName={person['sAMAccountName']})",
                attributes=["cn", "sn", "givenName", "sAMAccountName"],
            )
            assert len(response) == 1, (
                f"Expected 1 entry for {person['sAMAccountName']}, got {len(response)}"
            )

            entry = response[0]["attributes"]
            assert entry["cn"] == person["cn"] or entry["cn"] == [person["cn"]]
            assert entry["sn"] == person["sn"] or entry["sn"] == [person["sn"]]

    finally:
        for dn in created_dns:
            await ldap_connection.ldap_delete(combine_dn_strings(dn))


@pytest.mark.integration_test
@pytest.mark.envvar(SAMBA_ENVVARS)
@pytest.mark.usefixtures("test_client")
async def test_dirsync_detects_changes(
    context: Context,
    ldap_suffix: list[str],
    ldap_org_unit: list[str],
) -> None:
    """Test that Microsoft DirSync control works with Samba AD.

    DirSync (LDAP_SERVER_DIRSYNC_OID 1.2.840.113556.1.4.841) is an AD-specific
    control for incremental synchronization. This test verifies that:
    1. An initial DirSync returns existing entries and a cookie
    2. After creating a user, an incremental DirSync (with cookie) returns only
       the new entry
    3. The cookie is updated after each sync
    """
    ldap_connection = context["user_context"]["dataloader"].ldapapi.ldap_connection
    # DirSync requires the search base to be a naming context (root DN)
    naming_context = combine_dn_strings(ldap_suffix)

    # DirSync needs a plain sync connection (not SAFE_RESTARTABLE)
    server = ldap3.Server(SAMBA_HOST, port=SAMBA_PORT, get_info=ldap3.ALL)
    conn = ldap3.Connection(
        server,
        user=SAMBA_ENVVARS["LDAP_USER"],
        password=SAMBA_ENVVARS["LDAP_PASSWORD"],
        auto_bind=True,
        raise_exceptions=True,
    )

    # Initial DirSync to get a baseline cookie
    dir_sync = conn.extend.microsoft.dir_sync(
        sync_base=naming_context,
        sync_filter="(objectClass=user)",
        attributes=["sAMAccountName", "objectGUID"],
    )
    dir_sync.loop()
    initial_cookie = dir_sync.cookie
    assert initial_cookie is not None, "DirSync should return a cookie"

    # Create a new person via the app's connection
    person_dn = ["CN=DirSync Test User"] + ldap_org_unit
    await ldap_connection.ldap_add(
        combine_dn_strings(person_dn),
        object_class=["top", "person", "organizationalPerson", "user"],
        attributes={
            "objectClass": ["top", "person", "organizationalPerson", "user"],
            "cn": "DirSync Test User",
            "sn": "User",
            "givenName": "DirSync Test",
            "sAMAccountName": "dirsynctest",
            "userPrincipalName": "dirsynctest@magenta.dk",
        },
    )

    try:
        # Use DirSync with the previous cookie to get only changes
        dir_sync_incremental = conn.extend.microsoft.dir_sync(
            sync_base=naming_context,
            sync_filter="(objectClass=user)",
            attributes=["sAMAccountName", "objectGUID"],
            cookie=initial_cookie,
        )
        dir_sync_incremental.loop()

        # Verify the new entry appears in the incremental results
        changed_sam_names = set()
        for entry in conn.response:
            if entry["type"] != "searchResEntry":
                continue
            sam = entry["attributes"].get("sAMAccountName")
            if sam:
                changed_sam_names.add(sam)

        assert "dirsynctest" in changed_sam_names, (
            f"Expected 'dirsynctest' in DirSync changes, got: {changed_sam_names}"
        )

        # Verify cookie was updated
        assert dir_sync_incremental.cookie is not None
        assert dir_sync_incremental.cookie != initial_cookie

    finally:
        await ldap_connection.ldap_delete(combine_dn_strings(person_dn))
        conn.unbind()
