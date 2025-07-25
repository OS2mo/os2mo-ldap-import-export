# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import datetime
from typing import TypeAlias
from uuid import UUID
from uuid import uuid4

import pytest
from structlog.testing import capture_logs

from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    EmployeeCreateInput,
)
from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    EngagementCreateInput,
)
from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    ManagerCreateInput,
)
from mo_ldap_import_export.depends import GraphQLClient
from mo_ldap_import_export.environments.main import get_manager_person_uuid
from mo_ldap_import_export.types import EmployeeUUID
from mo_ldap_import_export.types import EngagementUUID
from mo_ldap_import_export.types import ManagerUUID

CreateEngagement: TypeAlias = Callable[
    [datetime | None, datetime | None], Awaitable[EngagementUUID]
]


@pytest.fixture
async def create_engagement(
    graphql_client: GraphQLClient,
    mo_person: UUID,
    mo_org_unit: UUID,
    ansat: UUID,
    jurist: UUID,
    primary: UUID,
) -> CreateEngagement:
    async def inner(start: datetime | None, end: datetime | None) -> EngagementUUID:
        engagement = await graphql_client.engagement_create(
            input=EngagementCreateInput(
                user_key="engagement",
                person=mo_person,
                org_unit=mo_org_unit,
                engagement_type=ansat,
                job_function=jurist,
                primary=primary,
                validity={"from": start, "to": end},
            )
        )
        return EngagementUUID(engagement.uuid)

    return inner


CreateManager: TypeAlias = Callable[
    [EmployeeUUID | None, datetime | None, datetime | None], Awaitable[ManagerUUID]
]


@pytest.fixture
async def create_manager(
    graphql_client: GraphQLClient, mo_org_unit: UUID
) -> CreateManager:
    async def inner(
        person: EmployeeUUID | None, start: datetime | None, end: datetime | None
    ) -> ManagerUUID:
        manager = await graphql_client._testing__manager_create(
            ManagerCreateInput(
                user_key="vacant",
                org_unit=mo_org_unit,
                responsibility=[],
                manager_level=uuid4(),
                manager_type=uuid4(),
                person=person,
                validity={"from": start, "to": end},
            )
        )
        return ManagerUUID(manager.uuid)

    return inner


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_get_manager_person_uuid_invalid_engagement(
    graphql_client: GraphQLClient,
) -> None:
    with capture_logs() as cap_logs:
        result = await get_manager_person_uuid(
            graphql_client, engagement_uuid=EngagementUUID(uuid4())
        )
    assert result is None

    events = [m["event"] for m in cap_logs]
    assert events == ["Invalid engagement"]


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_get_manager_person_uuid_past_engagement(
    graphql_client: GraphQLClient, create_engagement: CreateEngagement
) -> None:
    engagement_uuid = await create_engagement(
        datetime(2001, 2, 3, 4, 5, 6), datetime(2002, 3, 4, 5, 6, 7)
    )
    with capture_logs() as cap_logs:
        result = await get_manager_person_uuid(
            graphql_client, engagement_uuid=engagement_uuid
        )
    assert result is None

    events = [m["event"] for m in cap_logs]
    assert events == ["Invalid engagement"]


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_get_manager_person_uuid_future_engagement(
    graphql_client: GraphQLClient, create_engagement: CreateEngagement
) -> None:
    engagement_uuid = await create_engagement(datetime(3000, 1, 1), None)
    with capture_logs() as cap_logs:
        result = await get_manager_person_uuid(
            graphql_client, engagement_uuid=engagement_uuid
        )
    assert result is None

    events = [m["event"] for m in cap_logs]
    assert events == ["Invalid engagement"]


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_get_manager_person_uuid_current_engagement_no_manager(
    graphql_client: GraphQLClient, create_engagement: CreateEngagement
) -> None:
    engagement_uuid = await create_engagement(datetime(2000, 1, 1), None)
    with capture_logs() as cap_logs:
        result = await get_manager_person_uuid(
            graphql_client, engagement_uuid=engagement_uuid
        )
    assert result is None

    events = [m["event"] for m in cap_logs]
    assert events == ["No manager relation found"]


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_get_manager_person_uuid_current_engagement_self_manager(
    graphql_client: GraphQLClient,
    mo_person: EmployeeUUID,
    create_engagement: CreateEngagement,
    create_manager: CreateManager,
) -> None:
    engagement_uuid = await create_engagement(datetime(2000, 1, 1), None)
    await create_manager(mo_person, datetime(2000, 1, 1), None)

    with capture_logs() as cap_logs:
        result = await get_manager_person_uuid(
            graphql_client, engagement_uuid=engagement_uuid
        )
    assert result is None

    events = [m["event"] for m in cap_logs]
    assert events == ["No manager relation found"]


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_get_manager_person_uuid_current_engagement_vacant_manager(
    graphql_client: GraphQLClient,
    create_engagement: CreateEngagement,
    create_manager: CreateManager,
) -> None:
    engagement_uuid = await create_engagement(datetime(2000, 1, 1), None)
    await create_manager(None, datetime(2000, 1, 1), None)

    with capture_logs() as cap_logs:
        result = await get_manager_person_uuid(
            graphql_client, engagement_uuid=engagement_uuid
        )
    assert result is None

    events = [m["event"] for m in cap_logs]
    assert events == ["Vacant manager found"]


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_get_manager_person_uuid_current_engagement_past_manager(
    graphql_client: GraphQLClient,
    create_engagement: CreateEngagement,
    create_manager: CreateManager,
) -> None:
    engagement_uuid = await create_engagement(datetime(2000, 1, 1), None)

    manager_person = await graphql_client.person_create(
        input=EmployeeCreateInput(
            given_name="Boss",
            surname="Supervisor",
            cpr_number="0101701234",
        )
    )
    await create_manager(
        EmployeeUUID(manager_person.uuid), datetime(2001, 2, 3), datetime(2002, 3, 4)
    )

    with capture_logs() as cap_logs:
        result = await get_manager_person_uuid(
            graphql_client, engagement_uuid=engagement_uuid
        )
    assert result is None

    events = [m["event"] for m in cap_logs]
    assert events == ["No manager relation found"]


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_get_manager_person_uuid_current_engagement_future_manager(
    graphql_client: GraphQLClient,
    create_engagement: CreateEngagement,
    create_manager: CreateManager,
) -> None:
    engagement_uuid = await create_engagement(datetime(2000, 1, 1), None)
    manager_person = await graphql_client.person_create(
        input=EmployeeCreateInput(
            given_name="Boss",
            surname="Supervisor",
            cpr_number="0101701234",
        )
    )
    await create_manager(EmployeeUUID(manager_person.uuid), datetime(3000, 1, 1), None)

    with capture_logs() as cap_logs:
        result = await get_manager_person_uuid(
            graphql_client, engagement_uuid=engagement_uuid
        )
    assert result is None

    events = [m["event"] for m in cap_logs]
    assert events == ["No manager relation found"]


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_get_manager_person_uuid_current_engagement_current_manager(
    graphql_client: GraphQLClient,
    create_engagement: CreateEngagement,
    create_manager: CreateManager,
) -> None:
    engagement_uuid = await create_engagement(datetime(2000, 1, 1), None)
    manager_person = await graphql_client.person_create(
        input=EmployeeCreateInput(
            given_name="Boss",
            surname="Supervisor",
            cpr_number="0101701234",
        )
    )
    await create_manager(EmployeeUUID(manager_person.uuid), datetime(2000, 1, 1), None)

    result = await get_manager_person_uuid(
        graphql_client, engagement_uuid=engagement_uuid
    )
    assert result == manager_person.uuid


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_get_manager_person_uuid_current_engagement_current_manager_filtered(
    graphql_client: GraphQLClient,
    create_engagement: CreateEngagement,
    create_manager: CreateManager,
) -> None:
    engagement_uuid = await create_engagement(datetime(2000, 1, 1), None)
    manager_person = await graphql_client.person_create(
        input=EmployeeCreateInput(
            given_name="Boss",
            surname="Supervisor",
            cpr_number="0101701234",
        )
    )
    await create_manager(EmployeeUUID(manager_person.uuid), datetime(2000, 1, 1), None)

    with capture_logs() as cap_logs:
        result = await get_manager_person_uuid(
            graphql_client,
            engagement_uuid=engagement_uuid,
            filter={"uuids": [str(uuid4())]},
        )
    assert result is None

    events = [m["event"] for m in cap_logs]
    assert events == ["No manager relation found"]
