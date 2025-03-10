# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import call
from uuid import UUID

import pytest
from httpx import AsyncClient
from more_itertools import one

from mo_ldap_import_export.autogenerated_graphql_client import GraphQLClient
from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    AddressCreateInput,
)
from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    AddressUpdateInput,
)
from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    EmployeeCreateInput,
)
from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    OrganisationUnitCreateInput,
)


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.parametrize(
    "start,end",
    [
        # Current
        (datetime(1970, 1, 1), None),
        (datetime(1970, 1, 1), datetime(3000, 1, 1)),
        # Past
        pytest.param(
            datetime(1970, 1, 1),
            datetime(1980, 1, 1),
            marks=pytest.mark.xfail(reason="no events send"),
        ),
        # Future
        pytest.param(
            datetime(2900, 1, 1),
            datetime(3000, 1, 1),
            marks=pytest.mark.xfail(reason="no events send"),
        ),
    ],
)
async def test_address2person(
    test_client: AsyncClient,
    graphql_client: GraphQLClient,
    mo_person: UUID,
    email_employee: UUID,
    start: datetime | None,
    end: datetime | None,
) -> None:
    graphql_client.employee_refresh = AsyncMock()  # type: ignore

    address = await graphql_client.address_create(
        input=AddressCreateInput(
            user_key="address",
            person=mo_person,
            value="info@magenta.dk",
            address_type=email_employee,
            validity={"from": start, "to": end},
        )
    )

    result = await test_client.post(
        "/mo2ldap/address",
        headers={"Content-Type": "text/plain"},
        content=str(address.uuid),
    )
    result.raise_for_status()

    # Check that we send the event to MO
    graphql_client.employee_refresh.assert_awaited_once_with(
        "os2mo_ldap_ie", [mo_person]
    )


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.xfail(reason="no events send")
async def test_address2person_between(
    test_client: AsyncClient,
    graphql_client: GraphQLClient,
    mo_person: UUID,
    email_employee: UUID,
) -> None:
    graphql_client.employee_refresh = AsyncMock()  # type: ignore

    address = await graphql_client.address_create(
        input=AddressCreateInput(
            user_key="address",
            person=mo_person,
            value="info@magenta.dk",
            address_type=email_employee,
            validity={"from": datetime(2000, 1, 1), "to": datetime(2010, 1, 1)},
        )
    )
    await graphql_client.address_update(
        input=AddressUpdateInput(
            uuid=address.uuid,
            value="noreply@magenta.dk",
            address_type=email_employee,
            validity={"from": datetime(3000, 1, 1), "to": datetime(3010, 1, 1)},
        )
    )

    result = await test_client.post(
        "/mo2ldap/address",
        headers={"Content-Type": "text/plain"},
        content=str(address.uuid),
    )
    result.raise_for_status()

    # Check that we send the event to MO
    graphql_client.employee_refresh.assert_awaited_once_with(
        "os2mo_ldap_ie", [mo_person]
    )


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.xfail(reason="no events send")
async def test_address2person_change_person(
    test_client: AsyncClient,
    graphql_client: GraphQLClient,
    email_employee: UUID,
) -> None:
    graphql_client.employee_refresh = AsyncMock()  # type: ignore

    person1 = await graphql_client.user_create(
        input=EmployeeCreateInput(
            given_name="Aage",
            surname="Aagaard",
            cpr_number="0101010123",
        )
    )

    person2 = await graphql_client.user_create(
        input=EmployeeCreateInput(
            given_name="Betina",
            surname="Bundgaard",
            cpr_number="0101011234",
        )
    )

    address = await graphql_client.address_create(
        input=AddressCreateInput(
            user_key="address",
            person=person1.uuid,
            value="info@magenta.dk",
            address_type=email_employee,
            validity={"from": datetime(2000, 1, 1), "to": datetime(2010, 1, 1)},
        )
    )
    await graphql_client.address_update(
        input=AddressUpdateInput(
            uuid=address.uuid,
            person=person2.uuid,
            address_type=email_employee,
            validity={"from": datetime(3000, 1, 1), "to": datetime(3010, 1, 1)},
        )
    )

    result = await test_client.post(
        "/mo2ldap/address",
        headers={"Content-Type": "text/plain"},
        content=str(address.uuid),
    )
    result.raise_for_status()

    # Check that we send the event to MO
    args = one(graphql_client.employee_refresh.call_args_list).args
    exchange, uuids = args
    assert exchange == "os2mo_ldap_ie"
    assert set(uuids) == {person1.uuid, person2.uuid}


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.parametrize(
    "start,end",
    [
        # Current
        (datetime(1970, 1, 1), None),
        (datetime(1970, 1, 1), datetime(3000, 1, 1)),
        # Past
        pytest.param(
            datetime(1970, 1, 1),
            datetime(1980, 1, 1),
            marks=pytest.mark.xfail(reason="no events send"),
        ),
        # Future
        pytest.param(
            datetime(2900, 1, 1),
            datetime(3000, 1, 1),
            marks=pytest.mark.xfail(reason="no events send"),
        ),
    ],
)
async def test_address2orgunit(
    test_client: AsyncClient,
    graphql_client: GraphQLClient,
    mo_org_unit: UUID,
    email_unit: UUID,
    start: datetime | None,
    end: datetime | None,
) -> None:
    graphql_client.org_unit_engagements_refresh = AsyncMock()  # type: ignore

    address = await graphql_client.address_create(
        input=AddressCreateInput(
            user_key="address",
            org_unit=mo_org_unit,
            value="info@magenta.dk",
            address_type=email_unit,
            validity={"from": start, "to": end},
        )
    )

    result = await test_client.post(
        "/mo2ldap/address",
        headers={"Content-Type": "text/plain"},
        content=str(address.uuid),
    )
    result.raise_for_status()

    # Check that we send the event to MO
    graphql_client.org_unit_engagements_refresh.assert_awaited_once_with(
        "os2mo_ldap_ie", mo_org_unit
    )


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.xfail(reason="no events send")
async def test_address2orgunit_between(
    test_client: AsyncClient,
    graphql_client: GraphQLClient,
    mo_org_unit: UUID,
    email_unit: UUID,
) -> None:
    graphql_client.org_unit_engagements_refresh = AsyncMock()  # type: ignore

    address = await graphql_client.address_create(
        input=AddressCreateInput(
            user_key="address",
            org_unit=mo_org_unit,
            value="info@magenta.dk",
            address_type=email_unit,
            validity={"from": datetime(2000, 1, 1), "to": datetime(2010, 1, 1)},
        )
    )
    await graphql_client.address_update(
        input=AddressUpdateInput(
            uuid=address.uuid,
            value="noreply@magenta.dk",
            address_type=email_unit,
            validity={"from": datetime(3000, 1, 1), "to": datetime(3010, 1, 1)},
        )
    )

    result = await test_client.post(
        "/mo2ldap/address",
        headers={"Content-Type": "text/plain"},
        content=str(address.uuid),
    )
    result.raise_for_status()

    # Check that we send the event to MO
    graphql_client.org_unit_engagements_refresh.assert_awaited_once_with(
        "os2mo_ldap_ie", mo_org_unit
    )


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.xfail(reason="no events send")
async def test_address2orgunit_change_orgunit(
    test_client: AsyncClient,
    graphql_client: GraphQLClient,
    email_unit: UUID,
    afdeling: UUID,
) -> None:
    graphql_client.org_unit_engagements_refresh = AsyncMock()  # type: ignore

    org_unit1 = await graphql_client.org_unit_create(
        input=OrganisationUnitCreateInput(
            name="org_unit1",
            org_unit_type=afdeling,
            validity={"from": "1960-01-01T00:00:00Z"},
        )
    )
    org_unit2 = await graphql_client.org_unit_create(
        input=OrganisationUnitCreateInput(
            name="org_unit2",
            org_unit_type=afdeling,
            validity={"from": "1960-01-01T00:00:00Z"},
        )
    )

    address = await graphql_client.address_create(
        input=AddressCreateInput(
            user_key="address",
            org_unit=org_unit1.uuid,
            value="info@magenta.dk",
            address_type=email_unit,
            validity={"from": datetime(2000, 1, 1), "to": datetime(2010, 1, 1)},
        )
    )
    await graphql_client.address_update(
        input=AddressUpdateInput(
            uuid=address.uuid,
            org_unit=org_unit2.uuid,
            address_type=email_unit,
            validity={"from": datetime(3000, 1, 1), "to": datetime(3010, 1, 1)},
        )
    )

    result = await test_client.post(
        "/mo2ldap/address",
        headers={"Content-Type": "text/plain"},
        content=str(address.uuid),
    )
    result.raise_for_status()

    # Check that we send the event to MO
    graphql_client.org_unit_engagements_refresh.assert_has_calls(
        [call("os2mo_ldap_ie", org_unit1.uuid), call("os2mo_ldap_ie", org_unit2.uuid)],
        any_order=True,
    )
