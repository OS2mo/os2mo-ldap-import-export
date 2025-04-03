# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json
from collections.abc import Awaitable
from collections.abc import Callable
from typing import Any
from typing import cast
from unittest.mock import ANY
from uuid import UUID

import pytest
from fastramqpi.pytest_util import retry
from httpx import AsyncClient
from ldap3 import Connection
from more_itertools import one
from structlog.testing import capture_logs

from mo_ldap_import_export.autogenerated_graphql_client import EmployeeCreateInput
from mo_ldap_import_export.autogenerated_graphql_client import EmployeeFilter
from mo_ldap_import_export.autogenerated_graphql_client import EmployeeUpdateInput
from mo_ldap_import_export.autogenerated_graphql_client import GraphQLClient
from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    ITSystemFilter,
)
from mo_ldap_import_export.autogenerated_graphql_client.input_types import ITUserFilter
from mo_ldap_import_export.depends import Settings
from mo_ldap_import_export.ldap import get_ldap_object
from mo_ldap_import_export.ldap import ldap_add
from mo_ldap_import_export.ldap import ldap_modify
from mo_ldap_import_export.ldap import ldap_search
from mo_ldap_import_export.moapi import MOAPI
from mo_ldap_import_export.types import DN
from mo_ldap_import_export.types import EmployeeUUID
from mo_ldap_import_export.utils import combine_dn_strings
from mo_ldap_import_export.utils import mo_today


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "True",
        "CONVERSION_MAPPING": json.dumps(
            {
                "ldap_to_mo": {
                    "Employee": {
                        "objectClass": "Employee",
                        "_ldap_attributes_": ["employeeNumber", "givenName", "sn"],
                        "uuid": "{{ employee_uuid or '' }}",  # TODO: why is this required?
                        "cpr_number": "{{ ldap.employeeNumber }}",
                        "given_name": "{{ ldap.givenName }}",
                        "surname": "{{ ldap.sn }}",
                        "nickname_given_name": "foo",
                        "nickname_surname": "bar",
                    },
                },
                # TODO: why is this required?
                "username_generator": {
                    "combinations_to_try": ["FFFX", "LLLX"],
                },
            }
        ),
    }
)
async def test_to_mo(
    test_client: AsyncClient,
    graphql_client: GraphQLClient,
    mo_org_unit: UUID,
    ldap_connection: Connection,
    ldap_org_unit: list[str],
) -> None:
    cpr = "2108613133"

    @retry()
    async def assert_employee(expected: dict) -> None:
        employees = await graphql_client._testing__employee_read(
            filter=EmployeeFilter(cpr_numbers=[cpr])
        )
        employee = one(employees.objects)
        validities = one(employee.validities)
        assert validities.dict() == expected

    person_dn = combine_dn_strings(["uid=abk"] + ldap_org_unit)

    # LDAP: Create
    given_name = "create"
    await ldap_add(
        ldap_connection,
        dn=person_dn,
        object_class=["top", "person", "organizationalPerson", "inetOrgPerson"],
        attributes={
            "objectClass": ["top", "person", "organizationalPerson", "inetOrgPerson"],
            "ou": "os2mo",
            "cn": "Aage Bach Klarskov",
            "sn": "Bach Klarskov",
            "employeeNumber": cpr,
            "givenName": given_name,
        },
    )
    mo_employee = {
        "uuid": ANY,
        "user_key": ANY,
        "cpr_number": cpr,
        "given_name": given_name,
        "surname": "Bach Klarskov",
        "nickname_given_name": "foo",
        "nickname_surname": "bar",
    }
    await assert_employee(mo_employee)

    # LDAP: Edit
    given_name = "edit"
    await ldap_modify(
        ldap_connection,
        dn=person_dn,
        changes={
            "givenName": [("MODIFY_REPLACE", given_name)],
        },
    )
    mo_employee = {
        **mo_employee,
        "given_name": given_name,
    }
    await assert_employee(mo_employee)


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "True",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": json.dumps(
            {
                "mo2ldap": """
                    {% set mo_employee = load_mo_employee(uuid, current_objects_only=False) %}
                    {{
                        {
                            "employeeNumber": mo_employee.cpr_number,
                            "carLicense": mo_employee.uuid|string,
                            "uid": mo_employee.cpr_number,
                            "cn": mo_employee.given_name + " " + mo_employee.surname,
                            "sn": mo_employee.surname,
                            "givenName": mo_employee.given_name,
                            "displayName": mo_employee.nickname_given_name + " " + mo_employee.nickname_surname
                        }|tojson
                    }}
                """,
                # TODO: why is this required?
                "username_generator": {
                    "combinations_to_try": ["FFFX", "LLLX"],
                },
            }
        ),
    }
)
async def test_to_ldap(
    test_client: AsyncClient,
    graphql_client: GraphQLClient,
    mo_api: MOAPI,
    mo_org_unit: UUID,
    ldap_connection: Connection,
    ldap_org_unit: list[str],
) -> None:
    cpr = "2108613133"

    @retry()
    async def assert_employee(dn: str, expected: dict[str, Any]) -> None:
        response, _ = await ldap_search(
            ldap_connection,
            search_base=combine_dn_strings(ldap_org_unit),
            search_filter=f"(employeeNumber={cpr})",
            attributes=[
                "employeeNumber",
                "carLicense",
                "uid",
                "cn",
                "sn",
                "givenName",
                "displayName",
            ],
        )
        employee = one(response)
        assert employee["dn"] == dn
        assert employee["attributes"] == expected

    # MO: Create
    mo_employee = await graphql_client.user_create(
        input=EmployeeCreateInput(
            cpr_number=cpr,
            given_name="create",
            surname="Mustermann",
            nickname_given_name="Max",
            nickname_surname="Erika",
        )
    )
    await assert_employee(
        "cn=create Mustermann,ou=os2mo,o=magenta,dc=magenta,dc=dk",
        {
            "employeeNumber": "2108613133",
            "carLicense": [str(mo_employee.uuid)],
            "uid": ["2108613133"],
            "cn": ["create Mustermann"],
            "sn": ["Mustermann"],
            "givenName": ["create"],
            "displayName": "Max Erika",
        },
    )

    # MO: Edit
    await graphql_client.user_update(
        input=EmployeeUpdateInput(
            uuid=mo_employee.uuid,
            given_name="update",
            surname="Musterfrau",
            nickname_given_name="Manu",
            nickname_surname="Muster",
            validity={"from": "2011-12-13T14:15:16Z"},
            # TODO: why is this required?
            cpr_number=cpr,
        )
    )
    await assert_employee(
        "cn=update Musterfrau,ou=os2mo,o=magenta,dc=magenta,dc=dk",
        {
            "employeeNumber": "2108613133",
            "carLicense": [str(mo_employee.uuid)],
            "uid": ["2108613133"],
            "cn": ["update Musterfrau"],
            "sn": ["Musterfrau"],
            "givenName": ["update"],
            "displayName": "Manu Muster",
        },
    )


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "True",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": json.dumps(
            {
                "mo2ldap": """
                    {% set mo_employee = load_mo_employee(uuid, current_objects_only=False) %}
                    {{
                        {
                            "employeeNumber": mo_employee.cpr_number,
                            "uid": mo_employee.cpr_number,
                            "cn": mo_employee.given_name + " " + mo_employee.surname,
                            "sn": mo_employee.surname,
                        }|tojson
                    }}
                """,
                # TODO: why is this required?
                "username_generator": {
                    "combinations_to_try": ["FFFX", "LLLX"],
                },
            }
        ),
    }
)
@pytest.mark.parametrize(
    "rdn,expected",
    [
        ("uid=abk", "uid=2108613133"),
        ("cn=Aage Bach Klarskov", "cn=create Mustermann"),
    ],
)
async def test_edit_existing_in_ldap(
    test_client: AsyncClient,
    graphql_client: GraphQLClient,
    mo_api: MOAPI,
    mo_org_unit: UUID,
    ldap_connection: Connection,
    ldap_org_unit: list[str],
    rdn: str,
    expected: str,
) -> None:
    cpr = "2108613133"

    # Existing LDAP person has uid as part of the DN, but the mapping does not.
    person_dn = combine_dn_strings([rdn] + ldap_org_unit)

    # LDAP: Create
    await ldap_add(
        ldap_connection,
        dn=person_dn,
        object_class=["top", "person", "organizationalPerson", "inetOrgPerson"],
        attributes={
            "objectClass": ["top", "person", "organizationalPerson", "inetOrgPerson"],
            "ou": "os2mo",
            "cn": "Aage Bach Klarskov",
            "sn": "Bach Klarskov",
            "employeeNumber": cpr,
        },
    )

    # MO: Create
    await graphql_client.user_create(
        input=EmployeeCreateInput(
            cpr_number=cpr,
            given_name="create",
            surname="Mustermann",
            nickname_given_name="Max",
            nickname_surname="Erika",
        )
    )

    @retry()
    async def assert_employee() -> None:
        response, _ = await ldap_search(
            ldap_connection,
            search_base=combine_dn_strings(ldap_org_unit),
            search_filter=f"(employeeNumber={cpr})",
            attributes=[
                "employeeNumber",
                "uid",
                "cn",
                "sn",
            ],
        )
        employee = one(response)
        expected_dn = combine_dn_strings([expected] + ldap_org_unit)
        assert employee["dn"] == expected_dn
        assert employee["attributes"] == {
            "employeeNumber": "2108613133",
            "uid": ["2108613133"],
            "cn": ["create Mustermann"],
            "sn": ["Mustermann"],
        }

    await assert_employee()


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": json.dumps(
            {
                "mo2ldap": """
                    {% set mo_employee = load_mo_employee(uuid, current_objects_only=False) %}
                    {{
                        {
                            "carLicense": mo_employee.seniority,
                        }|tojson
                    }}
                """,
                # TODO: why is this required?
                "username_generator": {
                    "combinations_to_try": ["FFFX", "LLLX"],
                },
            }
        ),
    }
)
async def test_none_handling_empty(
    trigger_mo_person: Callable[[], Awaitable[None]],
    ldap_connection: Connection,
    ldap_person_dn: DN,
) -> None:
    ldap_object = await get_ldap_object(ldap_connection, ldap_person_dn)
    assert ldap_object.dn == ldap_person_dn
    assert hasattr(ldap_object, "carLicense") is False

    # As Seniority is None, the field should remain empty
    await trigger_mo_person()

    ldap_object = await get_ldap_object(ldap_connection, ldap_person_dn)
    assert ldap_object.dn == ldap_person_dn
    assert hasattr(ldap_object, "carLicense") is False


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": json.dumps(
            {
                "mo2ldap": """
                    {% set mo_employee = load_mo_employee(uuid, current_objects_only=False) %}
                    {{
                        {
                            "carLicense": mo_employee.seniority,
                        }|tojson
                    }}
                """,
                # TODO: why is this required?
                "username_generator": {
                    "combinations_to_try": ["FFFX", "LLLX"],
                },
            }
        ),
    }
)
async def test_none_handling_clearing(
    trigger_mo_person: Callable[[], Awaitable[None]],
    ldap_connection: Connection,
    ldap_person_dn: DN,
) -> None:
    await ldap_modify(
        ldap_connection,
        dn=ldap_person_dn,
        changes={
            "carLicense": [("MODIFY_REPLACE", "TEST_VALUE")],
        },
    )

    ldap_object = await get_ldap_object(ldap_connection, ldap_person_dn)
    assert ldap_object.dn == ldap_person_dn
    assert hasattr(ldap_object, "carLicense") is True
    assert getattr(ldap_object, "carLicense", None) == ["TEST_VALUE"]

    # As Seniority is None, the field should be cleared
    await trigger_mo_person()

    ldap_object = await get_ldap_object(ldap_connection, ldap_person_dn)
    assert ldap_object.dn == ldap_person_dn
    assert hasattr(ldap_object, "carLicense") is False


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "LDAP_IT_SYSTEM": "ADUUID",
        "CONVERSION_MAPPING": json.dumps(
            {
                "mo2ldap": """
                    {% set mo_employee = load_mo_employee(uuid, current_objects_only=False) %}
                    {{
                        {
                            "employeeNumber": mo_employee.cpr_number,
                            "uid": mo_employee.cpr_number,
                            "cn": mo_employee.given_name + " " + mo_employee.surname,
                            "sn": mo_employee.surname,
                            "givenName": mo_employee.given_name,
                            "displayName": mo_employee.nickname_given_name + " " + mo_employee.nickname_surname
                        }|tojson
                    }}
                """,
                # TODO: why is this required?
                "username_generator": {
                    "combinations_to_try": ["FFFX", "LLLX"],
                },
            }
        ),
    }
)
async def test_ituser_link(
    trigger_mo_person: Callable[[], Awaitable[None]],
    graphql_client: GraphQLClient,
    ldap_connection: Connection,
    ldap_org_unit: list[str],
) -> None:
    # Verify required settings are set
    settings = Settings()
    assert settings.ldap_it_system is not None
    assert settings.ldap_unique_id_field != ""

    # Trigger a sync creating a user in LDAP
    with capture_logs() as cap_logs:
        await trigger_mo_person()

    # Check that we are attempting to create an ITUser for correlation
    log_events = [x["event"] for x in cap_logs]
    assert "No ITUser found, creating one to correlate with DN" in log_events
    assert "LDAP UUID found for DN" in log_events

    # Fetch the LDAP UUID for the newly created LDAP user
    response, _ = await ldap_search(
        ldap_connection,
        search_base=combine_dn_strings(ldap_org_unit),
        search_filter="(employeeNumber=2108613133)",
        attributes=[settings.ldap_unique_id_field],
    )
    employee = one(response)
    ldap_uuid = employee["attributes"][settings.ldap_unique_id_field]

    # Check that an ITUser with the LDAP UUID as user-key was created
    users = await graphql_client._testing__ituser_read(
        filter=ITUserFilter(
            itsystem=ITSystemFilter(
                user_keys=[settings.ldap_it_system], from_date=None, to_date=None
            ),
            from_date=None,
            to_date=None,
        )
    )
    user = one(users.objects)
    user_key = one(user.validities).user_key
    assert user_key == ldap_uuid


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": json.dumps(
            {
                "mo2ldap": """
                    {% set mo_employee = load_mo_employee(uuid, current_objects_only=False) %}
                    {{
                        {
                            "employeeNumber": mo_employee.cpr_number,
                            "uid": mo_employee.cpr_number,
                            "cn": generate_common_name(uuid, dn),
                            "sn": mo_employee.surname,
                            "givenName": mo_employee.given_name,
                            "displayName": mo_employee.nickname_given_name + " " + mo_employee.nickname_surname
                        }|tojson
                    }}
                """,
                "username_generator": {
                    # TODO: why is this required?
                    "combinations_to_try": ["FFFX", "LLLX"],
                },
            }
        ),
    }
)
async def test_generate_common_name(
    trigger_sync: Callable[[EmployeeUUID], Awaitable[None]],
    graphql_client: GraphQLClient,
    ldap_connection: Connection,
    mo_person: EmployeeUUID,
    ldap_org_unit: list[str],
) -> None:
    async def fetch_common_name(cpr_number: str) -> str:
        # Fetch the LDAP UUID for the newly created LDAP user
        response, _ = await ldap_search(
            ldap_connection,
            search_base=combine_dn_strings(ldap_org_unit),
            search_filter=f"(employeeNumber={cpr_number})",
            attributes=["cn"],
        )
        employee = one(response)
        return cast(str, one(employee["attributes"]["cn"]))

    mo_person_cpr_number = "2108613133"

    # Trigger a sync creating a user in LDAP
    await trigger_sync(mo_person)
    common_name = await fetch_common_name(mo_person_cpr_number)
    assert common_name == "Aage Bach Klarskov"

    # Trigger a sync again, updating the user in LDAP
    # This should NOT yield an _2 name as we have the name already
    await trigger_sync(mo_person)
    common_name = await fetch_common_name(mo_person_cpr_number)
    assert common_name == "Aage Bach Klarskov"

    # Change the persons name, then trigger a sync again, updating the user in LDAP
    # This should change the common name to the new name
    await graphql_client.user_update(
        EmployeeUpdateInput(
            uuid=mo_person, surname="Klareng", validity={"from": mo_today()}
        )
    )
    await trigger_sync(mo_person)
    common_name = await fetch_common_name(mo_person_cpr_number)
    assert common_name == "Aage Klareng"

    # Create another person with the same name
    mo_person_2_cpr_number = "0101700000"
    r = await graphql_client.user_create(
        input=EmployeeCreateInput(
            given_name="Aage", surname="Klareng", cpr_number="0101700000"
        )
    )
    mo_person_2 = EmployeeUUID(r.uuid)
    await trigger_sync(mo_person_2)
    common_name = await fetch_common_name(mo_person_2_cpr_number)
    assert common_name == "Aage Klareng_2"

    # Trigger a sync again, updating the user in LDAP
    # This should yield the _2 name as that is what we already have
    await trigger_sync(mo_person_2)
    common_name = await fetch_common_name(mo_person_2_cpr_number)
    assert common_name == "Aage Klareng_2"

    # Trigger a sync again on the first person, updating the user in LDAP
    # This should NOT yield an _2 name as we have the name already
    await trigger_sync(mo_person)
    common_name = await fetch_common_name(mo_person_cpr_number)
    assert common_name == "Aage Klareng"
