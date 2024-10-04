# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
"""Integration tests."""

import json
from functools import partial
from uuid import UUID

import pytest
from httpx import AsyncClient
from ldap3 import Connection
from mergedeep import Strategy  # type: ignore
from mergedeep import merge  # type: ignore
from more_itertools import one

from mo_ldap_import_export.autogenerated_graphql_client import GraphQLClient
from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    AddressCreateInput,
)
from mo_ldap_import_export.ldap import ldap_modify
from mo_ldap_import_export.utils import combine_dn_strings

overlay = partial(merge, strategy=Strategy.TYPESAFE_ADDITIVE)


CONVERSION_MAPPING = {
    "ldap_to_mo": {
        "Employee": {
            "objectClass": "ramodels.mo.employee.Employee",
            "_import_to_mo_": "false",
            "uuid": "{{ employee_uuid or NONE }}",
            "cpr_no": "{{ldap.employeeNumber|strip_non_digits or NONE}}",
        },
        "PublicPhoneEmployee": {
            "objectClass": "ramodels.mo.details.address.Address",
            "_import_to_mo_": "true",
            "value": "{{ ldap.mobile or NONE }}",
            "address_type": "{{ dict(uuid=get_employee_address_type_uuid('PhoneEmployee')) }}",
            "person": "{{ dict(uuid=employee_uuid or NONE) }}",
            "visibility": "{{ dict(uuid=get_visibility_uuid('Public')) }}",
        },
        "InternalPhoneEmployee": {
            "objectClass": "ramodels.mo.details.address.Address",
            "_import_to_mo_": "true",
            "value": "{{ ldap.pager or NONE }}",
            "address_type": "{{ dict(uuid=get_employee_address_type_uuid('PhoneEmployee')) }}",
            "person": "{{ dict(uuid=employee_uuid or NONE) }}",
            "visibility": "{{ dict(uuid=get_visibility_uuid('Intern')) }}",
        },
    },
    "mo_to_ldap": {
        "Employee": {
            "objectClass": "inetOrgPerson",
            "_export_to_ldap_": "false",
            "employeeNumber": "{{mo_employee.cpr_no}}",
        },
        "PublicPhoneEmployee": {
            "objectClass": "inetOrgPerson",
            "_export_to_ldap_": "false",
            "mobile": "{{ mo_employee_address.value }}",
        },
        "InternalPhoneEmployee": {
            "objectClass": "inetOrgPerson",
            "_export_to_ldap_": "false",
            "pager": "{{ mo_employee_address.value }}",
        },
    },
    "username_generator": {
        "objectClass": "UserNameGenerator",
        "combinations_to_try": ["FFFX", "LLLX"],
    },
}


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "LISTEN_TO_CHANGES_IN_MO": "False",
    }
)
@pytest.mark.parametrize(
    "values",
    [
        pytest.param(
            {"12345678", "87654321", "11111111", "22222222"},
            marks=pytest.mark.envvar(
                {"CONVERSION_MAPPING": json.dumps(CONVERSION_MAPPING)}
            ),
        ),
        pytest.param(
            {"11111111", "22222222"},
            marks=pytest.mark.envvar(
                {
                    "CONVERSION_MAPPING": json.dumps(
                        overlay(
                            CONVERSION_MAPPING,
                            {
                                "ldap_to_mo": {
                                    "PublicPhoneEmployee": {
                                        "_mapper_": "{{ obj.visibility }}"
                                    },
                                    "InternalPhoneEmployee": {
                                        "_mapper_": "{{ obj.visibility }}"
                                    },
                                }
                            },
                        )
                    )
                }
            ),
        ),
    ],
)
async def test_mapping(
    test_client: AsyncClient,
    graphql_client: GraphQLClient,
    ldap_connection: Connection,
    ldap_person: list[str],
    mo_person: UUID,
    values: set[str],
) -> None:
    person_uuid = mo_person
    dn = combine_dn_strings(ldap_person)

    # Get UUID of the newly created LDAP user
    result = await test_client.get(f"/Inspect/dn2uuid/{dn}")
    assert result.status_code == 200
    ldap_user_uuid = UUID(result.json())

    # Fetch data in MO
    phone_employee_address_type_uuid = one(
        (
            await graphql_client.read_class_uuid_by_facet_and_class_user_key(
                "employee_address_type", "PhoneEmployee"
            )
        ).objects
    ).uuid
    public_visibility_uuid = one(
        (
            await graphql_client.read_class_uuid_by_facet_and_class_user_key(
                "visibility", "Public"
            )
        ).objects
    ).uuid
    internal_visibility_uuid = one(
        (
            await graphql_client.read_class_uuid_by_facet_and_class_user_key(
                "visibility", "Intern"
            )
        ).objects
    ).uuid

    await graphql_client._testing_address_create(
        input=AddressCreateInput(
            value="12345678",
            user_key="external_employee_phone",
            person=person_uuid,
            visibility=public_visibility_uuid,
            address_type=phone_employee_address_type_uuid,
            validity={"from": "1980-01-01T00:00:00Z"},
        )
    )
    await graphql_client._testing_address_create(
        input=AddressCreateInput(
            value="87654321",
            user_key="internal_employee_phone",
            person=person_uuid,
            visibility=internal_visibility_uuid,
            address_type=phone_employee_address_type_uuid,
            validity={"from": "1980-01-01T00:00:00Z"},
        )
    )

    # Verify addresses
    addresses = (
        await graphql_client.read_employee_addresses(
            employee_uuid=person_uuid,
            address_type_uuid=phone_employee_address_type_uuid,
        )
    ).objects
    address_values = {one(address.validities).value for address in addresses}
    assert address_values == {"12345678", "87654321"}

    # Setup data in LDAP
    changes = {"mobile": "11111111", "pager": "22222222"}
    for attribute, value in changes.items():
        await ldap_modify(
            ldap_connection, dn, changes={attribute: [("MODIFY_REPLACE", value)]}
        )

    # Trigger synchronization, we expect the addresses to be updated with new values
    content = str(ldap_user_uuid)
    headers = {"Content-Type": "text/plain"}
    result = await test_client.post("/ldap2mo/uuid", content=content, headers=headers)
    assert result.status_code == 200

    # Lookup the addresses above and see that they have been updated
    addresses = (
        await graphql_client.read_employee_addresses(
            employee_uuid=person_uuid,
            address_type_uuid=phone_employee_address_type_uuid,
        )
    ).objects
    address_values = {one(address.validities).value for address in addresses}
    assert address_values == values