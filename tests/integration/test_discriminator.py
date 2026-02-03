# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json
from uuid import uuid4

import pytest

from mo_ldap_import_export.config import Settings
from mo_ldap_import_export.exceptions import MultipleObjectsReturnedException
from mo_ldap_import_export.ldap import apply_discriminator
from mo_ldap_import_export.ldapapi import LDAPAPI
from mo_ldap_import_export.moapi import MOAPI
from mo_ldap_import_export.types import EmployeeUUID
from mo_ldap_import_export.utils import combine_dn_strings
from tests.integration.conftest import AddLdapPerson


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "DISCRIMINATOR_FIELDS": json.dumps(["uid"]),
        "DISCRIMINATOR_VALUES": json.dumps(
            [
                "{{ uid|length == 3 }}",
                "{{ uid|length == 4 }}",
                "{{ uid|length >= 5 }}",
            ]
        ),
    }
)
async def test_prefers_shorter_usernames(
    ldap_api: LDAPAPI,
    mo_api: MOAPI,
    add_ldap_person: AddLdapPerson,
) -> None:
    settings = Settings()

    ava = combine_dn_strings(await add_ldap_person("ava", "0101700000"))
    cleo = combine_dn_strings(await add_ldap_person("cleo", "0101700001"))
    emily = combine_dn_strings(await add_ldap_person("emily", "0101700002"))

    attributes = {"objectClass", settings.ldap_unique_id_field, "uid"}
    ldap_objects = await ldap_api.get_objects_by_dns({ava, cleo, emily}, attributes=attributes)

    result = await apply_discriminator(
        settings,
        ldap_api.ldap_connection.connection,
        mo_api,
        EmployeeUUID(uuid4()),
        ldap_objects,
    )
    assert result.dn == ava

    ldap_objects = [obj for obj in ldap_objects if obj.dn in {cleo, emily}]
    result = await apply_discriminator(
        settings,
        ldap_api.ldap_connection.connection,
        mo_api,
        EmployeeUUID(uuid4()),
        ldap_objects,
    )
    assert result.dn == cleo

    ldap_objects = [obj for obj in ldap_objects if obj.dn == emily]
    result = await apply_discriminator(
        settings,
        ldap_api.ldap_connection.connection,
        mo_api,
        EmployeeUUID(uuid4()),
        ldap_objects,
    )
    assert result.dn == emily


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "DISCRIMINATOR_FIELDS": json.dumps(["uid"]),
        "DISCRIMINATOR_VALUES": json.dumps(["{{ 'ass' not in dn }}"]),
    }
)
async def test_ignore_substring(
    ldap_api: LDAPAPI,
    mo_api: MOAPI,
    add_ldap_person: AddLdapPerson,
) -> None:
    settings = Settings()

    # Valid
    ava = combine_dn_strings(await add_ldap_person("ava", "0101700000"))
    cleo = combine_dn_strings(await add_ldap_person("cleo", "0101700001"))

    # Excluded by discriminator
    classic = combine_dn_strings(await add_ldap_person("classic", "0101700002"))
    grass = combine_dn_strings(await add_ldap_person("grass", "0101700003"))
    passenger = combine_dn_strings(await add_ldap_person("passenger", "0101700004"))
    assessment = combine_dn_strings(await add_ldap_person("assessment", "0101700005"))

    attributes = {"objectClass", settings.ldap_unique_id_field, "uid"}
    all_dns = {ava, cleo, classic, grass, passenger, assessment}
    all_objects = await ldap_api.get_objects_by_dns(all_dns, attributes=attributes)

    # No entries, returns None
    result = await apply_discriminator(
        settings,
        ldap_api.ldap_connection.connection,
        mo_api,
        EmployeeUUID(uuid4()),
        [],
    )
    assert result is None

    # One invalid, returns None
    invalid_objects = [obj for obj in all_objects if obj.dn == classic]
    result = await apply_discriminator(
        settings,
        ldap_api.ldap_connection.connection,
        mo_api,
        EmployeeUUID(uuid4()),
        invalid_objects,
    )
    assert result is None

    # Multiple invalid, returns None
    invalid_objects = [obj for obj in all_objects if obj.dn in {classic, grass, passenger}]
    result = await apply_discriminator(
        settings,
        ldap_api.ldap_connection.connection,
        mo_api,
        EmployeeUUID(uuid4()),
        invalid_objects,
    )
    assert result is None

    # Two valid means conflict
    valid_objects = [obj for obj in all_objects if obj.dn in {ava, cleo}]
    with pytest.raises(MultipleObjectsReturnedException) as exc_info:
        await apply_discriminator(
            settings,
            ldap_api.ldap_connection.connection,
            mo_api,
            EmployeeUUID(uuid4()),
            valid_objects,
        )
    assert "Ambiguous account result from apply discriminator" in str(exc_info.value)

    # One valid, one excluded returns the valid
    mixed_objects = [obj for obj in all_objects if obj.dn in {classic, ava}]
    result = await apply_discriminator(
        settings,
        ldap_api.ldap_connection.connection,
        mo_api,
        EmployeeUUID(uuid4()),
        mixed_objects,
    )
    assert result.dn == ava

    mixed_objects = [obj for obj in all_objects if obj.dn in {passenger, cleo}]
    result = await apply_discriminator(
        settings,
        ldap_api.ldap_connection.connection,
        mo_api,
        EmployeeUUID(uuid4()),
        mixed_objects,
    )
    assert result.dn == cleo

    # One valid, multiple excluded returns the valid
    mixed_objects = [obj for obj in all_objects if obj.dn in {ava, grass, assessment}]
    result = await apply_discriminator(
        settings,
        ldap_api.ldap_connection.connection,
        mo_api,
        EmployeeUUID(uuid4()),
        mixed_objects,
    )
    assert result.dn == ava

    mixed_objects = [obj for obj in all_objects if obj.dn in {cleo, classic, passenger}]
    result = await apply_discriminator(
        settings,
        ldap_api.ldap_connection.connection,
        mo_api,
        EmployeeUUID(uuid4()),
        mixed_objects,
    )
    assert result.dn == cleo

    # Multiple valid, multiple invalid means conflict
    with pytest.raises(MultipleObjectsReturnedException) as exc_info:
        await apply_discriminator(
            settings,
            ldap_api.ldap_connection.connection,
            mo_api,
            EmployeeUUID(uuid4()),
            all_objects,
        )
    assert "Ambiguous account result from apply discriminator" in str(exc_info.value)
