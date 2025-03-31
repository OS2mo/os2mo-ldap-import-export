# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from collections.abc import Awaitable
from collections.abc import Callable
from functools import partial
from typing import Any
from typing import TypeAlias
from uuid import UUID

import pytest
from fastramqpi.pytest_util import retry
from httpx import AsyncClient
from ldap3 import Connection
from more_itertools import one

from mo_ldap_import_export.autogenerated_graphql_client import EmployeeCreateInput
from mo_ldap_import_export.autogenerated_graphql_client import GraphQLClient
from mo_ldap_import_export.autogenerated_graphql_client import (
    OrganisationUnitCreateInput,
)
from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    EmployeeFilter,
)
from mo_ldap_import_export.depends import Settings
from mo_ldap_import_export.ldap import ldap_add
from mo_ldap_import_export.ldap import ldap_search
from mo_ldap_import_export.ldapapi import LDAPAPI
from mo_ldap_import_export.types import DN
from mo_ldap_import_export.types import LDAPUUID
from mo_ldap_import_export.types import EmployeeUUID
from mo_ldap_import_export.utils import combine_dn_strings

DNList2UUID: TypeAlias = Callable[[list[str]], Awaitable[UUID]]


@pytest.fixture
async def dnlist2uuid(ldap_api: LDAPAPI) -> DNList2UUID:
    async def inner(dnlist: list[str]) -> UUID:
        dn = combine_dn_strings(dnlist)
        return await ldap_api.get_ldap_unique_ldap_uuid(dn)

    return inner


@pytest.fixture
def ldap_suffix() -> list[str]:
    return ["dc=magenta", "dc=dk"]


@pytest.fixture
async def ldap_org_unit(
    ldap_connection: Connection, ldap_suffix: list[str]
) -> list[str]:
    o_dn = ["o=magenta"] + ldap_suffix
    await ldap_add(
        ldap_connection,
        combine_dn_strings(o_dn),
        object_class=["top", "organization"],
        attributes={"objectClass": ["top", "organization"], "o": "magenta"},
    )
    ou_dn = ["ou=os2mo"] + o_dn
    await ldap_add(
        ldap_connection,
        combine_dn_strings(ou_dn),
        object_class=["top", "organizationalUnit"],
        attributes={"objectClass": ["top", "organizationalUnit"], "ou": "os2mo"},
    )
    return ou_dn


@pytest.fixture
async def ldap_org_unit_uuid(
    ldap_org_unit: list[str], dnlist2uuid: DNList2UUID
) -> UUID:
    return await dnlist2uuid(ldap_org_unit)


AddLdapPerson: TypeAlias = Callable[[str, str], Awaitable[list[str]]]


@pytest.fixture
async def add_ldap_person(
    ldap_connection: Connection, ldap_org_unit: list[str]
) -> AddLdapPerson:
    async def adder(identifier: str, cpr_number: str) -> list[str]:
        person_dn = ["uid=" + identifier] + ldap_org_unit
        await ldap_add(
            ldap_connection,
            combine_dn_strings(person_dn),
            object_class=["top", "person", "organizationalPerson", "inetOrgPerson"],
            attributes={
                "objectClass": [
                    "top",
                    "person",
                    "organizationalPerson",
                    "inetOrgPerson",
                ],
                "uid": identifier,
                "cn": "cn",
                "givenName": "givenName",
                "sn": "sn",
                "ou": "os2mo",
                "mail": identifier + "@ad.kolding.dk",
                "userPassword": "{SSHA}j3lBh1Seqe4rqF1+NuWmjhvtAni1JC5A",
                "employeeNumber": cpr_number,
                "title": "title",
            },
        )
        return person_dn

    return adder


@pytest.fixture
async def ldap_person(
    ldap_connection: Connection, ldap_org_unit: list[str]
) -> list[str]:
    person_dn = ["uid=abk"] + ldap_org_unit
    await ldap_add(
        ldap_connection,
        combine_dn_strings(person_dn),
        object_class=["top", "person", "organizationalPerson", "inetOrgPerson"],
        attributes={
            "objectClass": ["top", "person", "organizationalPerson", "inetOrgPerson"],
            "uid": "abk",
            "cn": "Aage Bach Klarskov",
            "givenName": "Aage",
            "sn": "Bach Klarskov",
            "ou": "os2mo",
            "mail": "abk@ad.kolding.dk",
            "userPassword": "{SSHA}j3lBh1Seqe4rqF1+NuWmjhvtAni1JC5A",
            "employeeNumber": "2108613133",
            "title": "Skole underviser",
        },
    )
    return person_dn


@pytest.fixture
async def ldap_person_uuid(ldap_person: list[str], dnlist2uuid: DNList2UUID) -> UUID:
    return await dnlist2uuid(ldap_person)


@pytest.fixture
async def ldap_person_dn(ldap_person: list[str]) -> DN:
    return combine_dn_strings(ldap_person)


@pytest.fixture
async def mo_person(graphql_client: GraphQLClient) -> UUID:
    r = await graphql_client.user_create(
        input=EmployeeCreateInput(
            given_name="Aage",
            surname="Bach Klarskov",
            cpr_number="2108613133",
        )
    )
    return r.uuid


@pytest.fixture
async def afdeling(graphql_client: GraphQLClient) -> UUID:
    return one(
        (
            await graphql_client.read_class_uuid_by_facet_and_class_user_key(
                "org_unit_type", "Afdeling"
            )
        ).objects
    ).uuid


@pytest.fixture
async def mo_org_unit(graphql_client: GraphQLClient, afdeling: UUID) -> UUID:
    r = await graphql_client.org_unit_create(
        input=OrganisationUnitCreateInput(
            user_key="os2mo",
            name="os2mo",
            parent=None,
            org_unit_type=afdeling,
            validity={"from": "1960-01-01T00:00:00Z"},
        )
    )
    return r.uuid


@pytest.fixture
async def ansat(graphql_client: GraphQLClient) -> UUID:
    return one(
        (
            await graphql_client.read_class_uuid_by_facet_and_class_user_key(
                "engagement_type", "Ansat"
            )
        ).objects
    ).uuid


@pytest.fixture
async def jurist(graphql_client: GraphQLClient) -> UUID:
    return one(
        (
            await graphql_client.read_class_uuid_by_facet_and_class_user_key(
                "engagement_job_function", "Jurist"
            )
        ).objects
    ).uuid


@pytest.fixture
async def primary(graphql_client: GraphQLClient) -> UUID:
    return one(
        (
            await graphql_client.read_class_uuid_by_facet_and_class_user_key(
                "primary_type", "primary"
            )
        ).objects
    ).uuid


@pytest.fixture
async def non_primary(graphql_client: GraphQLClient) -> UUID:
    return one(
        (
            await graphql_client.read_class_uuid_by_facet_and_class_user_key(
                "primary_type", "non-primary"
            )
        ).objects
    ).uuid


@pytest.fixture
async def email_employee(graphql_client: GraphQLClient) -> UUID:
    return one(
        (
            await graphql_client.read_class_uuid_by_facet_and_class_user_key(
                "employee_address_type", "EmailEmployee"
            )
        ).objects
    ).uuid


@pytest.fixture
async def phone_employee(graphql_client: GraphQLClient) -> UUID:
    return one(
        (
            await graphql_client.read_class_uuid_by_facet_and_class_user_key(
                "employee_address_type", "PhoneEmployee"
            )
        ).objects
    ).uuid


@pytest.fixture
async def email_unit(graphql_client: GraphQLClient) -> UUID:
    return one(
        (
            await graphql_client.read_class_uuid_by_facet_and_class_user_key(
                "org_unit_address_type", "EmailUnit"
            )
        ).objects
    ).uuid


@pytest.fixture
async def public(graphql_client: GraphQLClient) -> UUID:
    return one(
        (
            await graphql_client.read_class_uuid_by_facet_and_class_user_key(
                "visibility", "Public"
            )
        ).objects
    ).uuid


@pytest.fixture
async def intern(graphql_client: GraphQLClient) -> UUID:
    return one(
        (
            await graphql_client.read_class_uuid_by_facet_and_class_user_key(
                "visibility", "Intern"
            )
        ).objects
    ).uuid


@pytest.fixture
async def adtitle(graphql_client: GraphQLClient) -> UUID:
    return one((await graphql_client.read_itsystem_uuid("ADtitle")).objects).uuid


@pytest.fixture
async def trigger_sync(
    test_client: AsyncClient,
) -> Callable[[EmployeeUUID], Awaitable[None]]:
    async def inner(uuid: EmployeeUUID) -> None:
        content = str(uuid)
        headers = {"Content-Type": "text/plain"}
        result = await test_client.post(
            "/mo2ldap/person", content=content, headers=headers
        )
        assert result.status_code == 200, result.text

    return inner


@pytest.fixture
async def trigger_ldap_sync(
    test_client: AsyncClient,
) -> Callable[[LDAPUUID], Awaitable[None]]:
    async def inner(uuid: LDAPUUID) -> None:
        content = str(uuid)
        headers = {"Content-Type": "text/plain"}
        result = await test_client.post(
            "/ldap2mo/uuid", content=content, headers=headers
        )
        assert result.status_code == 200, result.text

    return inner


@pytest.fixture
async def trigger_mo_person(
    trigger_sync: Callable[[EmployeeUUID], Awaitable[None]],
    mo_person: EmployeeUUID,
) -> Callable[[], Awaitable[None]]:
    return partial(trigger_sync, mo_person)


@pytest.fixture
async def trigger_ldap_person(
    trigger_ldap_sync: Callable[[LDAPUUID], Awaitable[None]],
    ldap_person_uuid: LDAPUUID,
) -> Callable[[], Awaitable[None]]:
    return partial(trigger_ldap_sync, ldap_person_uuid)


@pytest.fixture
async def assert_mo_person(
    mo_person: UUID, graphql_client: GraphQLClient
) -> Callable[[dict[str, Any]], Awaitable[None]]:
    @retry()
    async def assert_employee(expected: dict[str, Any]) -> None:
        employees = await graphql_client._testing__employee_read(
            filter=EmployeeFilter(uuids=[mo_person])
        )
        employee = one(employees.objects)
        validities = [validity.dict() for validity in employee.validities]
        assert validities == [expected]

    return assert_employee


@pytest.fixture
async def assert_ldap_person(
    ldap_person_uuid: LDAPUUID,
    ldap_org_unit: list[str],
    ldap_connection: Connection,
) -> Callable[[dict[str, Any]], Awaitable[None]]:
    settings = Settings()

    @retry()
    async def assert_employee(expected: dict[str, Any]) -> None:
        response, _ = await ldap_search(
            ldap_connection,
            search_base=combine_dn_strings(ldap_org_unit),
            search_filter=f"({settings.ldap_unique_id_field}={ldap_person_uuid})",
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
        assert employee["attributes"] == expected

    return assert_employee
