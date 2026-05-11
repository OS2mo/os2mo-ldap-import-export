# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import pytest

from mo_ldap_import_export.environments.main import get_ldap_attribute_values_by_cpr
from mo_ldap_import_export.ldapapi import LDAPAPI
from mo_ldap_import_export.types import DN
from mo_ldap_import_export.utils import combine_dn_strings
from tests.integration.conftest import AddLdapPerson


@pytest.mark.integration_test
@pytest.mark.xfail(reason="string value is coerced into a set", strict=True)
async def test_single_valued_string_attribute_is_not_exploded(
    ldap_api: LDAPAPI,
    ldap_person_dn: DN,
) -> None:
    # ldap3 unpacks SINGLE-VALUE fields into strings instead of lists.
    await ldap_api.ldap_connection.ldap_modify(
        dn=ldap_person_dn,
        changes={"displayName": [("MODIFY_REPLACE", "DEADCODE,03")]},
    )

    result = await get_ldap_attribute_values_by_cpr(
        ldap_api, "2108613133", "displayName"
    )
    assert result == {"DEADCODE,03"}


@pytest.mark.integration_test
async def test_multi_valued_attribute_returns_all_values(
    ldap_api: LDAPAPI,
    ldap_person_dn: DN,
) -> None:
    await ldap_api.ldap_connection.ldap_modify(
        dn=ldap_person_dn,
        changes={"carLicense": [("MODIFY_REPLACE", ["AAA-111", "BBB-222"])]},
    )

    result = await get_ldap_attribute_values_by_cpr(
        ldap_api, "2108613133", "carLicense"
    )
    assert result == {"AAA-111", "BBB-222"}


@pytest.mark.integration_test
@pytest.mark.usefixtures("ldap_person")
async def test_unset_attribute_yields_no_values(ldap_api: LDAPAPI) -> None:
    result = await get_ldap_attribute_values_by_cpr(
        ldap_api, "2108613133", "displayName"
    )
    assert result == set()


@pytest.mark.integration_test
async def test_overlapping_values_across_accounts_are_deduplicated(
    ldap_api: LDAPAPI,
    add_ldap_person: AddLdapPerson,
) -> None:
    cpr = "0101700000"
    acc1_dn = combine_dn_strings(await add_ldap_person("acc1", cpr))
    acc2_dn = combine_dn_strings(await add_ldap_person("acc2", cpr))
    acc3_dn = combine_dn_strings(await add_ldap_person("acc3", cpr))
    await ldap_api.ldap_connection.ldap_modify(
        dn=acc1_dn, changes={"carLicense": [("MODIFY_REPLACE", ["shared"])]}
    )
    await ldap_api.ldap_connection.ldap_modify(
        dn=acc2_dn, changes={"carLicense": [("MODIFY_REPLACE", ["shared"])]}
    )
    await ldap_api.ldap_connection.ldap_modify(
        dn=acc3_dn, changes={"carLicense": [("MODIFY_REPLACE", ["unique"])]}
    )

    result = await get_ldap_attribute_values_by_cpr(ldap_api, cpr, "carLicense")
    assert result == {"shared", "unique"}
