# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
# -*- coding: utf-8 -*-

import pytest
from ldap3.core.exceptions import LDAPInvalidDnError

from mo_ldap_import_export.types import DN
from mo_ldap_import_export.types import RDN
from mo_ldap_import_export.utils import combine_dn_strings
from mo_ldap_import_export.utils import dn_in_subtree
from mo_ldap_import_export.utils import extract_ou_from_dn
from mo_ldap_import_export.utils import import_class
from mo_ldap_import_export.utils import remove_vowels


async def test_import_class() -> None:
    imported_class = import_class("Employee")
    assert imported_class.__name__ == "Employee"

    with pytest.raises(NotImplementedError) as exc_info:
        import_class("Ashbringer")
    assert "Unknown argument to import_class" in str(exc_info.value)


@pytest.mark.parametrize(
    "parts,dn",
    [
        (["CN=Nick", "", "DC=bar"], "CN=Nick,DC=bar"),
        (["CN=Nick", "OU=f", "DC=bar"], "CN=Nick,OU=f,DC=bar"),
        (["CN=Nick", "DC=bar"], "CN=Nick,DC=bar"),
        (["cn=Nick+uid=unique_id", "DC=bar"], "cn=Nick+uid=unique_id,DC=bar"),
        (["CN=van Dyck, Jeff", "DC=bar"], "CN=van Dyck\\, Jeff,DC=bar"),
        (["CN=van=Dyck=Jeff", "DC=bar"], "CN=van\\=Dyck\\=Jeff,DC=bar"),
        (
            ["", "CN=van=Dy+uid=ck, Jeff", "", "", "DC=bar", ""],
            "CN=van\\=Dy+uid=ck\\, Jeff,DC=bar",
        ),
    ],
)
def test_combine_dn_strings(parts: list[RDN], dn: DN) -> None:
    assert combine_dn_strings(parts) == dn


def test_remove_vowels() -> None:
    assert remove_vowels("food") == "fd"


@pytest.mark.parametrize(
    "dn,subtree,expected",
    [
        # The subtree itself is considered to be within the subtree
        ("DC=magenta,DC=dk", "DC=magenta,DC=dk", True),
        ("UID=abk,OU=os2mo,DC=magenta,DC=dk", "DC=magenta,DC=dk", True),
        ("UID=abk,OU=os2mo,DC=magenta,DC=dk", "OU=os2mo,DC=magenta,DC=dk", True),
        ("UID=abk,OU=unmanaged,DC=magenta,DC=dk", "OU=os2mo,DC=magenta,DC=dk", False),
        # Only entire RDNs count, not string prefixes thereof
        ("UID=abk,OU=os2mo_extra,DC=magenta,DC=dk", "OU=os2mo,DC=magenta,DC=dk", False),
        # The subtree cannot be deeper than the DN itself
        ("DC=magenta,DC=dk", "OU=os2mo,DC=magenta,DC=dk", False),
        # Matching is case-insensitive, both on attribute-types and -values
        ("uid=abk,ou=OS2MO,dc=Magenta,dc=DK", "OU=os2mo,DC=magenta,DC=dk", True),
        # An empty subtree is the entire directory
        ("UID=abk,DC=magenta,DC=dk", "", True),
    ],
)
def test_dn_in_subtree(dn: DN, subtree: DN, expected: bool) -> None:
    assert dn_in_subtree(dn, subtree) is expected


def test_extract_ou_from_dn() -> None:
    assert extract_ou_from_dn("CN=Nick,OU=org,OU=main org,DC=f") == "OU=org,OU=main org"
    assert extract_ou_from_dn("CN=Nick,OU=org,DC=f") == "OU=org"
    assert extract_ou_from_dn("CN=Nick,DC=f") == ""

    with pytest.raises(LDAPInvalidDnError):
        extract_ou_from_dn("CN=Nick,OU=foo, DC=f")

    with pytest.raises(LDAPInvalidDnError):
        extract_ou_from_dn("")
