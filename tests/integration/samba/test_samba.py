# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Integration tests for Samba AD DC."""

from collections.abc import Callable
from collections.abc import Iterator
from contextlib import suppress

import ldap3
import pytest
from ldap3.utils.dn import parse_dn
from more_itertools import one

from mo_ldap_import_export.config import Settings
from mo_ldap_import_export.ldap import is_uuid
from mo_ldap_import_export.ldap import ldapresponse2entries
from mo_ldap_import_export.ldapapi import LDAPAPI
from mo_ldap_import_export.types import DN
from mo_ldap_import_export.utils import combine_dn_strings
from tests.integration.conftest import DN2UUID

from .conftest import SAMBA_BASELINE_USER_DNS


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


def _is_sid(value: str) -> bool:
    """Check if a string is a valid Windows domain SID.

    SIDs are a proprietary Microsoft format, not defined by any RFC.
    Format: S-1-IdentifierAuthority-SubAuthority1-SubAuthority2-...-SubAuthorityN

    See: https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/78eb9013-1c3a-4970-ad1f-2b1dad588a25

    For well-known values (5=SECURITY_NT_AUTHORITY, 21=SECURITY_NT_NON_UNIQUE):
    See: https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/81d92bba-d22b-4a8c-908a-554ab29148ab

    Args:
        value: String to validate as a SID.

    Returns:
        True if the string matches the expected SID format.
    """
    parts = value.split("-")
    if len(parts) != 8:
        return False
    prefix, revision, authority, sub_authority, d1, d2, d3, rid = parts
    return (
        # Literal SID prefix
        prefix == "S"
        # SID revision level (always 1)
        and revision == "1"
        # NT Authority
        and authority == "5"
        # SECURITY_NT_NON_UNIQUE (domain SIDs)
        and sub_authority == "21"
        and d1.isdigit()
        and d2.isdigit()
        and d3.isdigit()
        and rid.isdigit()
    )


def _is_dn(value: str) -> bool:
    """Check if a string is a valid LDAP DN.

    Args:
        value: String to validate as a DN.

    Returns:
        True if the string can be parsed as an LDAP DN.
    """
    with suppress(Exception):
        parse_dn(value)
        return True
    return False


def _parse_extended_dn(extended_dn: str) -> DN:
    """Parse an AD extended DN and return the real DN.

    Extended DNs are a proprietary Microsoft format (LDAP_SERVER_EXTENDED_DN_OID)
    that prepends GUID and SID to the standard RFC 2253 DN:
        `<GUID=...>;<SID=...>;CN=Foo,CN=Users,DC=magenta,DC=dk`

    See: https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-adts/57056773-932c-4e55-9491-e13f49ba580c

    Args:
        extended_dn: Extended DN string from a DirSync response.

    Returns:
        The plain LDAP DN extracted from the extended format.
    """
    guid_part, sid_part, dn = extended_dn.split(";")
    # Verify that this is a valid extended DN
    guid = guid_part.removeprefix("<GUID=").removesuffix(">")
    sid = sid_part.removeprefix("<SID=").removesuffix(">")
    assert is_uuid(guid), guid
    assert _is_sid(sid), sid
    assert _is_dn(dn), dn
    return dn


def _dirsync_dns(connection: ldap3.Connection) -> set[DN]:
    """Extract DNs from DirSync results.

    Args:
        connection: ldap3 connection with a completed DirSync response.

    Returns:
        Set of plain LDAP DNs parsed from the extended DN format.
    """
    entries = ldapresponse2entries(connection.response or [])
    extended_dns = {e["dn"] for e in entries}
    return {_parse_extended_dn(edn) for edn in extended_dns}


@pytest.fixture
def dirsync_connection() -> Iterator[ldap3.Connection]:
    """Dedicated ldap3 connection for DirSync using the default SYNC strategy.

    Cannot use SAFE_RESTARTABLE (as the production code does) because silent
    reconnects can interfere with reading connection.response after dir_sync calls.
    """
    settings = Settings()
    controller = one(settings.ldap_controllers)
    server = ldap3.Server(controller.host, port=controller.port, get_info=ldap3.ALL)
    with ldap3.Connection(
        server,
        user=settings.ldap_user,
        password=settings.ldap_password.get_secret_value(),
        auto_bind=True,
        raise_exceptions=True,
    ) as connection:
        yield connection


@pytest.fixture
def dirsync_loop(
    dirsync_connection: ldap3.Connection,
    ldap_suffix: list[str],
) -> Callable[[bytes | None], tuple[set[DN], bytes]]:
    """Return a callable that runs one DirSync polling loop."""
    search_base = combine_dn_strings(ldap_suffix)
    attrs = ["sn", "sAMAccountName", "userPrincipalName"]

    def _loop(cookie: bytes | None) -> tuple[set[DN], bytes]:
        ds = dirsync_connection.extend.microsoft.dir_sync(
            sync_base=search_base,
            sync_filter="(objectClass=user)",
            attributes=attrs,
            cookie=cookie,
        )
        ds.loop()
        return _dirsync_dns(dirsync_connection), ds.cookie

    return _loop


@pytest.mark.integration_test
@pytest.mark.usefixtures("test_client")
async def test_dirsync_detects_changes(
    ldap_api: LDAPAPI,
    dn2uuid: DN2UUID,
    dirsync_loop: Callable[[bytes | None], tuple[set[DN], bytes]],
    ldap_org_unit: list[str],
) -> None:
    """Test that Microsoft DirSync control works with Samba AD.

    DirSync (LDAP_SERVER_DIRSYNC_OID 1.2.840.113556.1.4.841) is a proprietary
    Microsoft LDAP control for incremental synchronization.
    See: https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-adts/2213a7f2-0a36-483c-b2a4-8574d53aa1e3

    This test walks through a full lifecycle:
    1. Initial full sync
       DirSync returns all existing entries
    2. Create first user
       DirSync finds exactly that user
    3. Create second user
       DirSync finds exactly that user
    4. Create two more users at once
       DirSync finds both
    5. Modify a tracked attribute
       DirSync picks up the change
    6. Modify an untracked attribute
       DirSync ignores it
    7. No changes
       DirSync returns nothing
    8. Delete a user
       DirSync reports the deletion
    """

    async def add_user(name: str) -> DN:
        dn = combine_dn_strings([f"CN={name}"] + ldap_org_unit)
        await ldap_api.add_ldap_object(
            dn,
            object_class="user",
            attributes={
                "sn": ["Test"],
                "sAMAccountName": [name],
                "userPrincipalName": [f"{name}@magenta.dk"],
            },
        )
        return dn

    # 1. Initial full synchronization
    initial_dns, cookie = dirsync_loop(None)
    assert cookie is not None
    # Without a cookie, DirSync returns all existing entries
    assert initial_dns >= SAMBA_BASELINE_USER_DNS
    # There may be extra entries from previous test runs, in which case they
    # should only be tombstones. AD moves deleted objects to CN=Deleted Objects
    # and removes them on a garbage collection schedule (default 180 days)
    # outside our control.
    tombstones = initial_dns - SAMBA_BASELINE_USER_DNS
    assert all("CN=Deleted Objects" in dn for dn in tombstones)

    # 2. Create first user
    alice = await add_user("Alice")
    changed, cookie = dirsync_loop(cookie)
    assert changed == {alice}

    # 3. Create second user
    bob = await add_user("Bob")
    changed, cookie = dirsync_loop(cookie)
    assert changed == {bob}

    # 4. Create two users at once
    charlie = await add_user("Charlie")
    eve = await add_user("Eve")
    changed, cookie = dirsync_loop(cookie)
    assert changed == {charlie, eve}

    # 5. Modify a tracked attribute
    await ldap_api.modify_ldap_object(alice, {"sn": ["Modified"]})

    changed, cookie = dirsync_loop(cookie)
    assert changed == {alice}

    # 6. Modify an untracked attribute
    await ldap_api.modify_ldap_object(alice, {"carLicense": ["XYZ-123"]})
    changed, cookie = dirsync_loop(cookie)
    assert changed == set()

    # 7. No changes at all
    changed, cookie = dirsync_loop(cookie)
    assert changed == set()

    # 8. Delete a user
    eve_uuid = await dn2uuid(eve)
    await ldap_api.ldap_connection.ldap_delete(eve)

    changed, cookie = dirsync_loop(cookie)
    # AD moves deleted objects to CN=Deleted Objects with the objectGUID in the DN
    tombstone_dn = f"CN=Eve\\0ADEL:{eve_uuid},CN=Deleted Objects,DC=magenta,DC=dk"
    assert changed == {tombstone_dn}
