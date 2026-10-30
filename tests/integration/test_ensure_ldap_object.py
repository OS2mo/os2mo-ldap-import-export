# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0

from typing import Any

import pytest
from fastramqpi.context import Context
from structlog.testing import capture_logs

from mo_ldap_import_export.config import Settings
from mo_ldap_import_export.exceptions import DryRunException
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


@pytest.mark.integration_test
@pytest.mark.parametrize(
    "old_uids,new_uids,expected_details",
    [
        # Changes to multi-valued attributes are reported with a diff
        (
            ["a", "b"],
            ["b", "c"],
            {
                "attributes": {"member": ["b", "c"]},
                "old_attributes": {"member": ["a", "b"]},
                "multivalued_diff": {
                    "member": {"added": ["c"], "removed": ["a"], "unchanged": 1}
                },
            },
        ),
        # Shrinking to a single value is still a multi-valued change
        (
            ["a", "b"],
            ["b"],
            {
                "attributes": {"member": ["b"]},
                "old_attributes": {"member": ["a", "b"]},
                "multivalued_diff": {
                    "member": {"added": [], "removed": ["a"], "unchanged": 1}
                },
            },
        ),
        # Single-valued changes are reported without a diff
        (
            ["a"],
            ["b"],
            {
                "attributes": {"member": ["b"]},
                "old_attributes": {"member": ["a"]},
            },
        ),
    ],
)
async def test_ensure_ldap_object_dry_run_diff(
    ldap_api: LDAPAPI,
    ldap_org_unit: list[str],
    old_uids: list[str],
    new_uids: list[str],
    expected_details: dict,
) -> None:
    def uid2dn(uid: str) -> str:
        return combine_dn_strings([f"uid={uid}"] + ldap_org_unit)

    def to_dns(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: to_dns(inner) for key, inner in value.items()}
        if isinstance(value, list):
            return [uid2dn(uid) for uid in value]
        return value

    group_dn = combine_dn_strings(["cn=group"] + ldap_org_unit)
    await ldap_api.ldap_connection.ldap_add(
        group_dn, "groupOfNames", attributes={"member": to_dns(old_uids)}
    )

    with pytest.raises(DryRunException) as exc_info:
        await ldap_api.ensure_ldap_object(
            group_dn,
            {"member": to_dns(new_uids)},
            "groupOfNames",
            create=False,
            dry_run=True,
        )
    assert exc_info.value.detail == {
        "message": "Would have changed attributes",
        "dn": group_dn,
        **to_dns(expected_details),
    }
