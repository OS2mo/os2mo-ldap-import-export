# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
# -*- coding: utf-8 -*-
from collections import defaultdict
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from fastramqpi.context import Context

from mo_ldap_import_export.customer_specific_checks import ExportChecks
from mo_ldap_import_export.dataloaders import DataLoader
from mo_ldap_import_export.depends import GraphQLClient
from mo_ldap_import_export.exceptions import IgnoreChanges
from mo_ldap_import_export.exceptions import UUIDNotFoundException
from tests.graphql_mocker import GraphQLMocker


@pytest.fixture
def context(dataloader: MagicMock) -> Context:
    user_context = {"dataloader": dataloader}

    return Context(user_context=user_context)


@pytest.fixture
def export_checks(dataloader: MagicMock) -> ExportChecks:
    return ExportChecks(dataloader)


async def test_check_it_user(graphql_mock: GraphQLMocker) -> None:
    graphql_client = GraphQLClient("http://example.com/graphql")
    context: dict[str, Any] = defaultdict(MagicMock)
    context["graphql_client"] = graphql_client
    amqpsystem = AsyncMock()

    dataloader = DataLoader(context, amqpsystem)  # type: ignore
    export_checks = ExportChecks(dataloader)

    route1 = graphql_mock.query("read_itsystem_uuid")
    route1.result = {"itsystems": {"objects": []}}

    route2 = graphql_mock.query("read_ituser_by_employee_and_itsystem_uuid")
    route2.result = {"itusers": {"objects": []}}

    route3 = graphql_mock.query("read_itusers")
    route3.result = {"itusers": {"objects": []}}

    employee_uuid = uuid4()

    # If the user_key attribute is empty, no exception should be raised
    await export_checks.check_it_user(employee_uuid, "")

    exc_info: pytest.ExceptionInfo

    # If the itsystem cannot be loaded, an exception should be raised
    with pytest.raises(UUIDNotFoundException) as exc_info:
        await export_checks.check_it_user(employee_uuid, "__non_existing")
    assert "itsystem not found, user_key: __non_existing" in str(exc_info.value)

    # If the itsystem exists, but the user does not have an ituser
    itsystem_uuid = uuid4()
    route1.result = {"itsystems": {"objects": [{"uuid": itsystem_uuid}]}}
    with pytest.raises(IgnoreChanges) as exc_info:
        await export_checks.check_it_user(employee_uuid, "foo")
    assert (
        f"employee with uuid = {employee_uuid} does not have an it-user with user_key = foo"
        in str(exc_info.value)
    )

    # If the user has an ituser, no exception should be raised
    route2.result = {"itusers": {"objects": [{"uuid": uuid4()}]}}
    route3.result = {
        "itusers": {
            "objects": [
                {
                    "validities": [
                        {
                            "user_key": "myituser",
                            "validity": {"from": "1900-01-01T00:00:00Z"},
                            "employee_uuid": employee_uuid,
                            "itsystem_uuid": itsystem_uuid,
                        }
                    ]
                }
            ]
        }
    }
    await export_checks.check_it_user(employee_uuid, "foo")
