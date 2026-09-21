# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json
from typing import Any

import pytest
from more_itertools import one

from mo_ldap_import_export.ldapapi import LDAPAPI
from mo_ldap_import_export.types import DN
from mo_ldap_import_export.types import CPRNumber
from mo_ldap_import_export.utils import combine_dn_strings


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


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LDAP_OUS_TO_SEARCH_IN": json.dumps(
            ["ou=os2mo,o=magenta", "ou=filtered,o=magenta"]
        ),
    }
)
async def test_cpr2dns_finds_account_when_multiple_ous_are_configured(
    ldap_api: LDAPAPI,
    ldap_org: list[str],
    ldap_org_unit: list[str],
) -> None:
    """Reproduces a bug where an account living in a second, non-default OU
    (e.g. a "filtered" OU that also happens to be in scope for
    LDAP_OUS_TO_SEARCH_IN) is not found by `cpr2dns`, even though a direct
    LDAP search for the same filter/base finds it fine.

    Root cause: `object_search` (mo_ldap_import_export/ldap.py) builds
    `ChainMap(searchParameters, {"search_base": search_base})` per OU, but
    `searchParameters` (the first/priority mapping) already has its own
    `search_base` key set to the *entire list* of OUs, so the per-iteration
    override is never actually applied -- every iteration searches with the
    same (invalid) list-valued `search_base` instead of the intended single
    OU, and the account is never found.
    """
    cpr_number = "2108613133"

    # Second OU that is in scope for LDAP_OUS_TO_SEARCH_IN
    filtered_ou = ["ou=filtered"] + ldap_org
    await ldap_api.ldap_connection.ldap_add(
        combine_dn_strings(filtered_ou),
        object_class=["top", "organizationalUnit"],
        attributes={"objectClass": ["top", "organizationalUnit"], "ou": "filtered"},
    )

    # Account living in the second OU only
    person_dn = ["cn=Alice Allman"] + filtered_ou
    await ldap_api.ldap_connection.ldap_add(
        combine_dn_strings(person_dn),
        object_class=["top", "person", "organizationalPerson", "inetOrgPerson"],
        attributes={
            "objectClass": ["top", "person", "organizationalPerson", "inetOrgPerson"],
            "employeeNumber": cpr_number,
            "givenName": "Alice",
            "sn": "Allman",
        },
    )

    results = await ldap_api.cpr2dns(CPRNumber(cpr_number), attributes=set())
    assert one(results).dn == combine_dn_strings(person_dn)
