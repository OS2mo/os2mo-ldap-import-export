# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from datetime import datetime
from unittest.mock import ANY
from uuid import UUID
from uuid import uuid4

import pytest
from fastramqpi.context import Context
from structlog.testing import capture_logs

from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    AddressCreateInput,
)
from mo_ldap_import_export.depends import GraphQLClient
from mo_ldap_import_export.environments.main import load_address
from mo_ldap_import_export.utils import MO_TZ


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_load_address(
    graphql_client: GraphQLClient,
    context: Context,
    mo_person: UUID,
    email_employee: UUID,
    public: UUID,
) -> None:
    mail = "create@example.com"
    await graphql_client.address_create(
        input=AddressCreateInput(
            user_key="test address",
            address_type=email_employee,
            value=mail,
            person=mo_person,
            visibility=public,
            validity={"from": "2001-02-03T04:05:06Z"},
        )
    )

    dataloader = context["user_context"]["dataloader"]
    result = await load_address(dataloader.moapi, mo_person, "EmailEmployee")
    assert result is not None
    assert result.dict(exclude_none=True) == {
        "visibility": public,
        "address_type": email_employee,
        "person": mo_person,
        "user_key": ANY,  # TODO: Why is this not "test_address"??
        "value": mail,
        "uuid": ANY,
        "validity": {"start": datetime(2001, 2, 3, 0, 0, tzinfo=MO_TZ)},
    }


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_load_address_deleted(
    graphql_client: GraphQLClient,
    context: Context,
    mo_person: UUID,
    email_employee: UUID,
    public: UUID,
) -> None:
    mail = "create@example.com"
    await graphql_client.address_create(
        input=AddressCreateInput(
            user_key="test address",
            address_type=email_employee,
            value=mail,
            person=mo_person,
            visibility=public,
            validity={"from": "2001-02-03T04:05:06Z", "to": "2002-03-04T05:06:07Z"},
        )
    )

    dataloader = context["user_context"]["dataloader"]
    with capture_logs() as cap_logs:
        result = await load_address(dataloader.moapi, mo_person, "EmailEmployee")
    assert result is None

    events = [m["event"] for m in cap_logs]
    assert events == [
        "Loading address",
        "Returning delete=True because to_date <= current_date",
        "Employee address is terminated",
    ]


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_load_address_multiple_matches(
    graphql_client: GraphQLClient,
    context: Context,
    mo_person: UUID,
    email_employee: UUID,
    public: UUID,
) -> None:
    mail = "create@example.com"
    await graphql_client.address_create(
        input=AddressCreateInput(
            user_key="address1",
            address_type=email_employee,
            value=mail,
            person=mo_person,
            visibility=public,
            validity={"from": "2001-02-03T04:05:06Z"},
        )
    )
    await graphql_client.address_create(
        input=AddressCreateInput(
            user_key="address2",
            address_type=email_employee,
            value=mail,
            person=mo_person,
            visibility=public,
            validity={"from": "2001-02-03T04:05:06Z"},
        )
    )

    dataloader = context["user_context"]["dataloader"]
    with pytest.raises(ValueError) as exc_info:
        await load_address(dataloader.moapi, mo_person, "EmailEmployee")
    assert "Expected exactly one item in iterable" in str(exc_info.value)


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_load_address_invalid_employee(context: Context) -> None:
    dataloader = context["user_context"]["dataloader"]
    employee_uuid = uuid4()
    with capture_logs() as cap_logs:
        result = await load_address(dataloader.moapi, employee_uuid, "EmailEmployee")
    assert result is None

    events = [m["event"] for m in cap_logs]
    assert events == ["Could not find employee address"]


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_load_address_invalid_address_type(
    context: Context, mo_person: UUID
) -> None:
    dataloader = context["user_context"]["dataloader"]
    with capture_logs() as cap_logs:
        result = await load_address(
            dataloader.moapi, mo_person, "non_existing_it_system"
        )
    assert result is None

    events = [m["event"] for m in cap_logs]
    assert events == ["Could not find employee address"]


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_load_address_no_address(context: Context, mo_person: UUID) -> None:
    dataloader = context["user_context"]["dataloader"]
    with capture_logs() as cap_logs:
        result = await load_address(dataloader.moapi, mo_person, "EmailEmployee")
    assert result is None

    events = [m["event"] for m in cap_logs]
    assert events == ["Could not find employee address"]


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_load_address_multiple_disjoint_matches(
    graphql_client: GraphQLClient,
    context: Context,
    mo_person: UUID,
    email_employee: UUID,
    public: UUID,
) -> None:
    await graphql_client.address_create(
        input=AddressCreateInput(
            user_key="address1",
            address_type=email_employee,
            value="address1@example.com",
            person=mo_person,
            visibility=public,
            validity={"from": "2001-02-03T04:05:06Z", "to": "2002-03-04T05:06:07Z"},
        )
    )
    await graphql_client.address_create(
        input=AddressCreateInput(
            user_key="address2",
            address_type=email_employee,
            value="address2@example.com",
            person=mo_person,
            visibility=public,
            validity={"from": "2003-04-05T06:07:08Z"},
        )
    )

    dataloader = context["user_context"]["dataloader"]
    result = await load_address(dataloader.moapi, mo_person, "EmailEmployee")
    assert result is not None
    assert result.value == "address2@example.com"
