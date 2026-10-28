# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0

import pytest
from fastramqpi.context import Context
from structlog.testing import capture_logs

from mo_ldap_import_export.config import Settings
from mo_ldap_import_export.ldap import get_ldap_object
from mo_ldap_import_export.ldapapi import LDAPAPI
from mo_ldap_import_export.utils import combine_dn_strings


@pytest.mark.integration_test
@pytest.mark.usefixtures("test_client")
async def test_ensure_ldap_object(
    context: Context,
    ldap_org_unit: list[str],
) -> None:
    ldapapi: LDAPAPI = context["user_context"]["dataloader"].ldapapi
    settings = Settings()

    # Create
    person_dn = combine_dn_strings(["cn=Aage"] + ldap_org_unit)
    current_dn = await ldapapi.ensure_ldap_object(
        person_dn,
        {"cn": ["Aage"], "sn": ["Bach"]},
        settings.ldap_object_class,
        create=True,
    )
    assert current_dn == person_dn
    ldap_obj = await get_ldap_object(ldapapi.connection, person_dn)
    expected = {
        "cn": ["Aage"],
        "dn": person_dn,
        "objectClass": ["inetOrgPerson"],
        "sn": ["Bach"],
    }
    assert ldap_obj.dict() == expected

    # Edit
    new_person_dn = combine_dn_strings(["cn=Anita"] + ldap_org_unit)
    current_dn = await ldapapi.ensure_ldap_object(
        person_dn,
        {"cn": ["Anita"], "uid": ["abk"]},
        settings.ldap_object_class,
        create=False,
    )
    assert current_dn == new_person_dn
    ldap_obj = await get_ldap_object(ldapapi.connection, new_person_dn)
    assert ldap_obj.dict() == {
        **expected,
        "cn": ["Anita"],
        "uid": ["abk"],
        "dn": new_person_dn,
    }


@pytest.mark.integration_test
@pytest.mark.xfail(strict=True, reason="reordered values are detected as changes")
async def test_ensure_ldap_object_reordered_values(
    ldap_api: LDAPAPI, ldap_org_unit: list[str]
) -> None:
    group_dn = combine_dn_strings(["cn=group"] + ldap_org_unit)
    members = [
        combine_dn_strings([f"uid={uid}"] + ldap_org_unit) for uid in ["a", "b", "c"]
    ]
    await ldap_api.ldap_connection.ldap_add(
        group_dn, "groupOfNames", attributes={"member": members}
    )

    with capture_logs() as cap_logs:
        current_dn = await ldap_api.ensure_ldap_object(
            group_dn,
            {"member": members[::-1]},
            "groupOfNames",
            create=False,
            dry_run=True,
        )
    assert current_dn == group_dn
    events = [m["event"] for m in cap_logs]
    assert "Not writing to LDAP as changeset is empty" in events
