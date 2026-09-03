# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Unit tests for the pure helpers of the DirSync event generator."""

from typing import Any
from uuid import uuid4

import pytest

from mo_ldap_import_export.dirsync_event_generator import collect_dirsync_entries
from mo_ldap_import_export.dirsync_event_generator import dirsync_entries_to_uuids
from mo_ldap_import_export.dirsync_event_generator import dn_in_search_bases
from mo_ldap_import_export.dirsync_event_generator import strip_extended_dn
from mo_ldap_import_export.types import LDAPUUID

USERS = "CN=Users,DC=magenta,DC=dk"
GUID_PREFIX = "<GUID=0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0>"
SID_PREFIX = "<SID=S-1-5-21-1-2-3-1105>"


@pytest.mark.parametrize(
    "dn,expected",
    [
        (f"CN=A,{USERS}", f"CN=A,{USERS}"),
        (f"{GUID_PREFIX};CN=A,{USERS}", f"CN=A,{USERS}"),
        (f"{GUID_PREFIX};{SID_PREFIX};CN=A,{USERS}", f"CN=A,{USERS}"),
    ],
)
def test_strip_extended_dn(dn: str, expected: str) -> None:
    assert strip_extended_dn(dn) == expected


@pytest.mark.parametrize(
    "dn,search_bases,expected",
    [
        # Direct child of the base
        (f"CN=A,{USERS}", [USERS], True),
        # The base itself
        (USERS, [USERS], True),
        # Nested below the base
        (f"CN=A,OU=Nested,{USERS}", [USERS], True),
        # Case differences are irrelevant
        ("cn=a,cn=users,dc=magenta,dc=dk", [USERS], True),
        # Extended DN prefixes are ignored
        (f"{GUID_PREFIX};{SID_PREFIX};CN=A,{USERS}", [USERS], True),
        # Sibling container
        ("CN=A,OU=Outside,DC=magenta,DC=dk", [USERS], False),
        # A container whose name merely ends with the base's RDN value
        ("CN=A,CN=OtherUsers,DC=magenta,DC=dk", [USERS], False),
        # The naming context itself as the base matches everything below it
        ("CN=A,OU=Outside,DC=magenta,DC=dk", ["DC=magenta,DC=dk"], True),
        # Any of several bases is enough
        (
            "CN=A,OU=Outside,DC=magenta,DC=dk",
            [USERS, "OU=Outside,DC=magenta,DC=dk"],
            True,
        ),
        # No bases means nothing is in scope
        (f"CN=A,{USERS}", [], False),
    ],
)
def test_dn_in_search_bases(dn: str, search_bases: list[str], expected: bool) -> None:
    assert dn_in_search_bases(dn, search_bases) is expected


def _entry(dn: str, **attributes: Any) -> dict[str, Any]:
    return {"type": "searchResEntry", "dn": dn, "attributes": attributes}


def test_dirsync_entries_to_uuids_scopes_live_and_deleted_objects() -> None:
    inside = str(uuid4())
    outside = str(uuid4())
    deleted_inside = str(uuid4())
    deleted_outside = str(uuid4())
    deleted_objects = "CN=Deleted Objects,DC=magenta,DC=dk"

    entries = [
        _entry(f"{GUID_PREFIX};{SID_PREFIX};CN=In,{USERS}", objectGUID=inside),
        _entry("CN=Out,OU=Outside,DC=magenta,DC=dk", objectGUID=outside),
        # Tombstones live under Deleted Objects; scope by lastKnownParent.
        # ldap3 decodes booleans when it has schema info, otherwise "TRUE".
        _entry(
            f"CN=Gone\\0ADEL:{deleted_inside},{deleted_objects}",
            objectGUID=deleted_inside,
            isDeleted=True,
            lastKnownParent=USERS,
        ),
        _entry(
            f"CN=GoneOut\\0ADEL:{deleted_outside},{deleted_objects}",
            objectGUID=deleted_outside,
            isDeleted="TRUE",
            lastKnownParent="OU=Outside,DC=magenta,DC=dk",
        ),
    ]

    result = dirsync_entries_to_uuids(entries, "objectGUID", [USERS])
    assert result == {LDAPUUID(inside), LDAPUUID(deleted_inside)}


def test_dirsync_entries_to_uuids_uses_configured_unique_id_field() -> None:
    uuid = str(uuid4())
    entries = [_entry(f"CN=In,{USERS}", objectGUID=str(uuid4()), entryUUID=uuid)]
    assert dirsync_entries_to_uuids(entries, "entryUUID", [USERS]) == {LDAPUUID(uuid)}


class FakeDirSyncPages:
    """Stand-in for ldap3's DirSync object, replaying scripted pages."""

    def __init__(self, pages: list[tuple[list[dict[str, Any]], bytes, bool]]):
        self._pages = iter(pages)
        self.more_results = True
        self.cookie = b"initial"

    def loop(self) -> list[dict[str, Any]]:
        page, self.cookie, self.more_results = next(self._pages)
        return page


def test_collect_dirsync_entries_drains_all_pages() -> None:
    pages = [
        ([_entry("CN=A"), {"type": "searchResRef", "uri": []}], b"cookie-1", True),
        # An empty page while the server still reports more results
        ([], b"cookie-2", True),
        ([_entry("CN=B")], b"cookie-3", False),
    ]
    entries, cookie = collect_dirsync_entries(FakeDirSyncPages(pages))
    assert [entry["dn"] for entry in entries] == ["CN=A", "CN=B"]
    assert cookie == b"cookie-3"


def test_collect_dirsync_entries_single_page() -> None:
    entries, cookie = collect_dirsync_entries(
        FakeDirSyncPages([([_entry("CN=A")], b"cookie-1", False)])
    )
    assert [entry["dn"] for entry in entries] == ["CN=A"]
    assert cookie == b"cookie-1"
