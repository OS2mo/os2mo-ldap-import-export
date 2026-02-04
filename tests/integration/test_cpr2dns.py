# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any

import pytest
from more_itertools import one

from mo_ldap_import_export.ldapapi import LDAPAPI
from mo_ldap_import_export.types import DN
from mo_ldap_import_export.types import CPRNumber


@pytest.mark.integration_test
@pytest.mark.usefixtures("ldap_org_unit", "ldap_person")
@pytest.mark.parametrize(
    "attributes, expected",
    [
        (set(), {}),
        ({"cn"}, {"cn": ["Aage Bach Klarskov"]}),
        (
            {"cn", "sn", "mail"},
            {
                "cn": ["Aage Bach Klarskov"],
                "sn": ["Bach Klarskov"],
                "mail": ["abk@ad.kolding.dk"],
            },
        ),
    ],
)
async def test_cpr2dns_attributes(
    ldap_api: LDAPAPI,
    ldap_person_dn: DN,
    attributes: set[str],
    expected: dict[str, Any],
) -> None:
    # CPR number for the ldap_person
    cpr_number = "2108613133"

    # Fetch with specific attributes
    results = await ldap_api.cpr2dns(CPRNumber(cpr_number), attributes=attributes)
    obj = one(results)

    # Verify that the dictionary representation matches exactly
    assert obj.dict() == {"dn": ldap_person_dn, **expected}
