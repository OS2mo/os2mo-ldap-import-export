# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from datetime import datetime
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from httpx import AsyncClient
from more_itertools import one

from mo_ldap_import_export.autogenerated_graphql_client import GraphQLClient
from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    EmployeeCreateInput,
)
from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    ITUserCreateInput,
)
from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    ITUserUpdateInput,
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
        (datetime(1970, 1, 1), datetime(1980, 1, 1)),
        # Future
        (
            datetime(2900, 1, 1),
            datetime(3000, 1, 1),
        ),
    ],
)
async def test_ituser2person(
    test_client: AsyncClient,
    graphql_client: GraphQLClient,
    mo_person: UUID,
    adtitle: UUID,
    start: datetime | None,
    end: datetime | None,
) -> None:
    graphql_client.employee_refresh = AsyncMock()  # type: ignore

    ituser = await graphql_client.ituser_create(
        input=ITUserCreateInput(
            user_key="ituser",
            person=mo_person,
            itsystem=adtitle,
            validity={"from": start, "to": end},
        )
    )

    result = await test_client.post(
        "/mo2ldap/ituser",
        headers={"Content-Type": "text/plain"},
        content=str(ituser.uuid),
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
async def test_ituser2person_between(
    test_client: AsyncClient,
    graphql_client: GraphQLClient,
    mo_person: UUID,
    adtitle: UUID,
) -> None:
    graphql_client.employee_refresh = AsyncMock()  # type: ignore

    ituser = await graphql_client.ituser_create(
        input=ITUserCreateInput(
            user_key="create",
            person=mo_person,
            itsystem=adtitle,
            validity={"from": datetime(2000, 1, 1), "to": datetime(2010, 1, 1)},
        )
    )
    await graphql_client.ituser_update(
        input=ITUserUpdateInput(
            uuid=ituser.uuid,
            user_key="update",
            itsystem=adtitle,
            validity={"from": datetime(3000, 1, 1), "to": datetime(3010, 1, 1)},
        )
    )

    result = await test_client.post(
        "/mo2ldap/ituser",
        headers={"Content-Type": "text/plain"},
        content=str(ituser.uuid),
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
async def test_ituser2person_change_person(
    test_client: AsyncClient,
    graphql_client: GraphQLClient,
    adtitle: UUID,
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

    ituser = await graphql_client.ituser_create(
        input=ITUserCreateInput(
            user_key="ituser",
            person=person1.uuid,
            itsystem=adtitle,
            validity={"from": datetime(2000, 1, 1), "to": datetime(2010, 1, 1)},
        )
    )
    await graphql_client.ituser_update(
        input=ITUserUpdateInput(
            uuid=ituser.uuid,
            person=person2.uuid,
            itsystem=adtitle,
            validity={"from": datetime(3000, 1, 1), "to": datetime(3010, 1, 1)},
        )
    )

    result = await test_client.post(
        "/mo2ldap/ituser",
        headers={"Content-Type": "text/plain"},
        content=str(ituser.uuid),
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
        (datetime(1970, 1, 1), datetime(1980, 1, 1)),
        # Future
        (datetime(2900, 1, 1), datetime(3000, 1, 1)),
    ],
)
async def test_ituser2orgunit(
    test_client: AsyncClient,
    graphql_client: GraphQLClient,
    mo_org_unit: UUID,
    adtitle: UUID,
    start: datetime | None,
    end: datetime | None,
) -> None:
    graphql_client.org_unit_refresh = AsyncMock()  # type: ignore

    ituser = await graphql_client.ituser_create(
        input=ITUserCreateInput(
            user_key="ituser",
            org_unit=mo_org_unit,
            itsystem=adtitle,
            validity={"from": start, "to": end},
        )
    )

    result = await test_client.post(
        "/mo2ldap/ituser",
        headers={"Content-Type": "text/plain"},
        content=str(ituser.uuid),
    )
    result.raise_for_status()

    # Check that we send the event to MO
    graphql_client.org_unit_refresh.assert_awaited_once_with(
        "os2mo_ldap_ie", [mo_org_unit]
    )


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
async def test_ituser2orgunit_between(
    test_client: AsyncClient,
    graphql_client: GraphQLClient,
    mo_org_unit: UUID,
    adtitle: UUID,
) -> None:
    graphql_client.org_unit_refresh = AsyncMock()  # type: ignore

    ituser = await graphql_client.ituser_create(
        input=ITUserCreateInput(
            user_key="create",
            org_unit=mo_org_unit,
            itsystem=adtitle,
            validity={"from": datetime(2000, 1, 1), "to": datetime(2010, 1, 1)},
        )
    )
    await graphql_client.ituser_update(
        input=ITUserUpdateInput(
            uuid=ituser.uuid,
            user_key="update",
            itsystem=adtitle,
            validity={"from": datetime(3000, 1, 1), "to": datetime(3010, 1, 1)},
        )
    )

    result = await test_client.post(
        "/mo2ldap/ituser",
        headers={"Content-Type": "text/plain"},
        content=str(ituser.uuid),
    )
    result.raise_for_status()

    # Check that we send the event to MO
    graphql_client.org_unit_refresh.assert_awaited_once_with(
        "os2mo_ldap_ie", [mo_org_unit]
    )


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
async def test_ituser2orgunit_change_orgunit(
    test_client: AsyncClient,
    graphql_client: GraphQLClient,
    adtitle: UUID,
    afdeling: UUID,
) -> None:
    graphql_client.org_unit_refresh = AsyncMock()  # type: ignore

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

    ituser = await graphql_client.ituser_create(
        input=ITUserCreateInput(
            user_key="ituser",
            org_unit=org_unit1.uuid,
            itsystem=adtitle,
            validity={"from": datetime(2000, 1, 1), "to": datetime(2010, 1, 1)},
        )
    )
    await graphql_client.ituser_update(
        input=ITUserUpdateInput(
            uuid=ituser.uuid,
            org_unit=org_unit2.uuid,
            itsystem=adtitle,
            validity={"from": datetime(3000, 1, 1), "to": datetime(3010, 1, 1)},
        )
    )

    result = await test_client.post(
        "/mo2ldap/ituser",
        headers={"Content-Type": "text/plain"},
        content=str(ituser.uuid),
    )
    result.raise_for_status()

    # Check that we send the event to MO
    args = one(graphql_client.org_unit_refresh.call_args_list).args
    exchange, uuids = args
    assert exchange == "os2mo_ldap_ie"
    assert set(uuids) == {org_unit1.uuid, org_unit2.uuid}
