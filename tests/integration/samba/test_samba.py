# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Integration tests for Samba AD DC (Active Directory compatible LDAP server)."""

import ldap3
import pytest
from fastramqpi.context import Context

from mo_ldap_import_export.utils import combine_dn_strings

from .conftest import SAMBA_ENVVARS

SAMBA_HOST = "samba"
SAMBA_PORT = 389


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

    # Read back and verify
    for person in persons:
        search_base = combine_dn_strings(ldap_org_unit)
        response, result = await ldap_connection.ldap_search(
            search_base=search_base,
            search_filter=f"(sAMAccountName={person['sAMAccountName']})",
            attributes=["cn", "sn", "givenName", "sAMAccountName"],
        )
        assert (
            len(response) == 1
        ), f"Expected 1 entry for {person['sAMAccountName']}, got {len(response)}"

        entry = response[0]["attributes"]
        assert entry["cn"] == person["cn"] or entry["cn"] == [person["cn"]]
        assert entry["sn"] == person["sn"] or entry["sn"] == [person["sn"]]


def _dirsync_entries(conn: ldap3.Connection) -> list[dict]:
    """Extract searchResEntry results from the last DirSync response."""
    return [e for e in conn.response if e["type"] == "searchResEntry"]


def _dirsync_dns(conn: ldap3.Connection) -> set[str]:
    """Extract the real DN (stripping GUID/SID prefixes) from DirSync results."""
    dns = set()
    for entry in _dirsync_entries(conn):
        # DirSync DNs look like "<GUID=...>;<SID=...>;CN=Foo,CN=Users,DC=..."
        dn = entry["dn"]
        # Strip extended DN components to get the real DN
        parts = dn.split(";")
        real_dn = parts[-1]
        dns.add(real_dn.lower())
    return dns


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
    control for incremental synchronization. This test walks through a full
    lifecycle:

    1. Initial DirSync returns existing entries and a cookie
    2. Create first user - incremental DirSync finds exactly that user
    3. Create second user - incremental DirSync finds exactly that user
    4. Create two more users at once - incremental DirSync finds both
    5. Modify a user - incremental DirSync picks up the change
    6. No changes - incremental DirSync returns nothing
    7. Delete a user - incremental DirSync reports the deletion
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

    dirsync_attrs = ["sAMAccountName", "sn", "objectGUID"]

    async def add_user(cn: str, sn: str, sam: str) -> list[str]:
        dn_parts = [f"CN={cn}"] + ldap_org_unit
        await ldap_connection.ldap_add(
            combine_dn_strings(dn_parts),
            object_class=["top", "person", "organizationalPerson", "user"],
            attributes={
                "objectClass": ["top", "person", "organizationalPerson", "user"],
                "cn": cn,
                "sn": sn,
                "sAMAccountName": sam,
                "userPrincipalName": f"{sam}@magenta.dk",
            },
        )
        return dn_parts

    def dirsync_loop(cookie: bytes | None) -> tuple[set[str], bytes]:
        """Run a DirSync loop, return (set of lowercase DNs, new cookie)."""
        ds = conn.extend.microsoft.dir_sync(
            sync_base=naming_context,
            sync_filter="(objectClass=user)",
            attributes=dirsync_attrs,
            cookie=cookie,
        )
        ds.loop()
        return _dirsync_dns(conn), ds.cookie

    def dn_of(dn_parts: list[str]) -> str:
        return combine_dn_strings(dn_parts).lower()

    # --- Step 1: Initial full sync -----------------------------------------
    _initial_dns, cookie = dirsync_loop(cookie=None)
    # Samba ships with built-in user accounts; we just need the cookie
    assert cookie is not None

    # --- Step 2: Create first user -----------------------------------------
    alice = await add_user("Alice Test", "Test", "alice_ds")
    changed, cookie = dirsync_loop(cookie)
    assert changed == {dn_of(alice)}

    # --- Step 3: Create second user ----------------------------------------
    bob = await add_user("Bob Test", "Test", "bob_ds")
    changed, cookie = dirsync_loop(cookie)
    assert changed == {dn_of(bob)}

    # --- Step 4: Create two users at once ----------------------------------
    charlie = await add_user("Charlie Test", "Test", "charlie_ds")
    diana = await add_user("Diana Test", "Test", "diana_ds")
    changed, cookie = dirsync_loop(cookie)
    assert changed == {dn_of(charlie), dn_of(diana)}

    # --- Step 5: Modify a user ---------------------------------------------
    conn.modify(
        combine_dn_strings(alice), {"sn": [(ldap3.MODIFY_REPLACE, ["Modified"])]}
    )
    assert conn.result["description"] == "success"

    changed, cookie = dirsync_loop(cookie)
    assert changed == {dn_of(alice)}
    # Verify the new attribute value is in the response
    for entry in _dirsync_entries(conn):
        real_dn = entry["dn"].split(";")[-1].lower()
        if real_dn == dn_of(alice):
            sn = entry["attributes"].get("sn", "")
            assert sn == "Modified" or sn == ["Modified"]
            break

    # --- Step 6: No changes ------------------------------------------------
    changed, cookie = dirsync_loop(cookie)
    assert changed == set()

    # --- Step 7: Delete a user ---------------------------------------------
    await ldap_connection.ldap_delete(combine_dn_strings(diana))

    changed, cookie = dirsync_loop(cookie)
    # Deleted objects are moved to CN=Deleted Objects so the DN changes;
    # verify exactly one entry referencing diana appeared
    entries = _dirsync_entries(conn)
    diana_entries = [e for e in entries if "diana test" in e["dn"].lower()]
    assert len(diana_entries) == 1
    assert changed == {diana_entries[0]["dn"].split(";")[-1].lower()}

    conn.unbind()
