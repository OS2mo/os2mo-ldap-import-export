# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Integration tests."""

from typing import Any
from uuid import uuid4

import pytest

from mo_ldap_import_export.autogenerated_graphql_client import GraphQLClient
from mo_ldap_import_export.autogenerated_graphql_client.input_types import AddressFilter
from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    EngagementFilter,
)
from mo_ldap_import_export.autogenerated_graphql_client.input_types import ITUserFilter


@pytest.mark.integration_test
@pytest.mark.usefixtures("test_client")
@pytest.mark.parametrize(
    "method_name,arguments",
    [
        (
            "read_facet_uuid",
            {"user_key": "test"},
        ),
        (
            "read_class_uuid",
            {"user_key": "test"},
        ),
        (
            "read_root_org_uuid",
            {},
        ),
        (
            "read_employees_with_engagement_to_org_unit",
            {"org_unit_uuid": uuid4()},
        ),
        (
            "read_engagements",
            {"uuids": [uuid4()]},
        ),
        (
            "read_engagements_by_employee_uuid",
            {"employee_uuid": uuid4()},
        ),
        (
            "read_engagements_by_engagements_filter",
            {"engagements_filter": EngagementFilter(uuids=[uuid4()])},
        ),
        (
            "read_employee_uuid_by_cpr_number",
            {"cpr_number": "0101700000"},
        ),
        (
            "read_employees",
            {"uuids": [uuid4()]},
        ),
        (
            "read_itusers",
            {"uuids": [uuid4()]},
        ),
        (
            "read_employee_uuid_by_ituser_user_key",
            {"user_key": "test"},
        ),
        (
            "read_ituser_by_employee_and_itsystem_uuid",
            {"employee_uuid": uuid4(), "itsystem_uuid": uuid4()},
        ),
        (
            "read_is_primary_engagements",
            {"uuids": [uuid4()]},
        ),
        (
            "read_employee_addresses",
            {"employee_uuid": uuid4(), "address_type_uuid": uuid4()},
        ),
        (
            "read_org_unit_addresses",
            {"org_unit_uuid": uuid4(), "address_type_uuid": uuid4()},
        ),
        (
            "read_class_uuid_by_facet_and_class_user_key",
            {"facet_user_key": "test", "class_user_key": "test"},
        ),
        (
            "read_class_name_by_class_uuid",
            {"class_uuid": uuid4()},
        ),
        (
            "read_addresses",
            {"uuids": [uuid4()]},
        ),
        (
            "read_filtered_addresses",
            {"filter": AddressFilter(uuids=[uuid4()])},
        ),
        (
            "read_filtered_itusers",
            {"filter": ITUserFilter(uuids=[uuid4()])},
        ),
        (
            "read_engagements_is_primary",
            {"filter": EngagementFilter(uuids=[uuid4()])},
        ),
        (
            "read_ituser_employee_uuid",
            {"ituser_uuid": uuid4()},
        ),
        (
            "read_engagement_employee_uuid",
            {"engagement_uuid": uuid4()},
        ),
        (
            "read_address_relation_uuids",
            {"address_uuid": uuid4()},
        ),
        (
            "read_all_ituser_user_keys_by_itsystem_uuid",
            {"itsystem_uuid": uuid4()},
        ),
        (
            "read_org_unit_name",
            {"org_unit_uuid": uuid4()},
        ),
        (
            "read_itsystem_uuid",
            {"user_key": "test"},
        ),
    ],
)
async def test_client_read_endpoints(
    graphql_client: GraphQLClient, method_name: str, arguments: dict[str, Any]
) -> None:
    """Ensure that calling our readers do not raise exceptions."""
    reader = getattr(graphql_client, method_name)
    await reader(**arguments)
