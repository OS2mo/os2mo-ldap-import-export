# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Literal

import pytest

from mo_ldap_import_export.ldapapi import LDAPAPI
from mo_ldap_import_export.types import DN
from mo_ldap_import_export.utils import combine_dn_strings


@pytest.mark.integration_test
@pytest.mark.parametrize(
    "search_filter, expected",
    [
        # The person, found in multiple ways
        ("(cn=Aage Bach Klarskov)", "person"),
        ("(mail=abk@ad.kolding.dk)", "person"),
        ("(uid=abk)", "person"),
        # The org unit
        ("(&(objectClass=organizationalUnit)(ou=os2mo))", "org"),
        # Object class filters out the otherwise matching person
        ("(&(objectClass=groupOfNames)(cn=Aage Bach Klarskov))", None),
        # No object matches
        ("(cn=Brittany Wilder)", None),
    ],
)
async def test_find_dns(
    ldap_api: LDAPAPI,
    ldap_person_dn: DN,
    ldap_org_unit: list[str],
    search_filter: str,
    expected: Literal["person", "org"] | None,
) -> None:
    expected_dns = set()
    if expected == "person":
        expected_dns = {ldap_person_dn}
    elif expected == "org":
        expected_dns = {combine_dn_strings(ldap_org_unit)}
    assert await ldap_api.find_dns(search_filter) == expected_dns
