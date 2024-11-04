# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json
from unittest.mock import ANY
from uuid import UUID

import pytest
from fastramqpi.pytest_util import retry
from httpx import AsyncClient
from ldap3 import Connection
from more_itertools import one

from mo_ldap_import_export.autogenerated_graphql_client import EmployeeFilter
from mo_ldap_import_export.autogenerated_graphql_client import EngagementFilter
from mo_ldap_import_export.autogenerated_graphql_client import GraphQLClient
from mo_ldap_import_export.ldap import ldap_add
from mo_ldap_import_export.ldap import ldap_modify
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
                        "objectClass": "ramodels.mo.employee.Employee",
                        "_import_to_mo_": "false",
                        "_ldap_attributes_": [],
                        "uuid": "{{ employee_uuid or '' }}",  # TODO: why is this required?
                    },
                    "Engagement": {
                        "objectClass": "ramodels.mo.details.engagement.Engagement",
                        "_import_to_mo_": "true",
                        "_ldap_attributes_": [
                            "carLicense",
                            "title",
                            "departmentNumber",
                        ],
                        "_mapper_": "{{ obj.org_unit }}",
                        # carLicense is arbitrarily chosen as an enabled/disabled marker
                        "_terminate_": "{{ now()|mo_datestring if ldap.carLicense == 'EXPIRED' else '' }}",
                        "user_key": "{{ ldap.title }}",
                        "person": "{{ employee_uuid }}",
                        "org_unit": "{{ ldap.departmentNumber }}",
                        "engagement_type": "{{ get_engagement_type_uuid('Ansat') }}",
                        "job_function": "{{ get_job_function_uuid('Jurist') }}",
                        "primary": "{{ get_primary_type_uuid('primary') }}",
                        "extension_1": "{{ ldap.title }}",
                    },
                },
                # TODO: why is this required?
                "username_generator": {
                    "objectClass": "UserNameGenerator",
                    "combinations_to_try": ["FFFX", "LLLX"],
                },
            }
        ),
    }
)
async def test_to_mo(
    test_client: AsyncClient,
    graphql_client: GraphQLClient,
    mo_person: UUID,
    mo_org_unit: UUID,
    ldap_connection: Connection,
    ldap_org: list[str],
) -> None:
    @retry()
    async def assert_engagement(expected: dict) -> None:
        engagements = await graphql_client._testing__engagement_read(
            filter=EngagementFilter(
                employee=EmployeeFilter(uuids=[mo_person]),
            ),
        )
        engagement = one(engagements.objects)
        validities = one(engagement.validities)
        assert validities.dict() == expected

    person_dn = combine_dn_strings(["uid=abk"] + ldap_org)

    # LDAP: Create
    title = "create"
    await ldap_add(
        ldap_connection,
        dn=person_dn,
        object_class=["top", "person", "organizationalPerson", "inetOrgPerson"],
        attributes={
            "objectClass": ["top", "person", "organizationalPerson", "inetOrgPerson"],
            "ou": "os2mo",
            "cn": "Aage Bach Klarskov",
            "sn": "Bach Klarskov",
            "employeeNumber": "2108613133",
            "carLicense": "ACTIVE",
            "title": title,
            "departmentNumber": str(mo_org_unit),
        },
    )
    mo_engagement = {
        "uuid": ANY,
        "user_key": title,
        "person": [{"uuid": mo_person}],
        "org_unit": [{"uuid": mo_org_unit}],
        "engagement_type": {"user_key": "Ansat"},
        "job_function": {"user_key": "Jurist"},
        "primary": {"user_key": "primary"},
        "extension_1": title,
        "validity": {"from_": mo_today(), "to": None},
    }
    await assert_engagement(mo_engagement)

    # LDAP: Edit
    title = "edit"
    await ldap_modify(
        ldap_connection,
        dn=person_dn,
        changes={
            "title": [("MODIFY_REPLACE", title)],
        },
    )
    mo_engagement = {
        **mo_engagement,
        "user_key": title,
        "extension_1": title,
    }
    await assert_engagement(mo_engagement)

    # LDAP: Terminate
    await ldap_modify(
        ldap_connection,
        dn=person_dn,
        changes={
            "carLicense": [("MODIFY_REPLACE", "EXPIRED")],
        },
    )
    mo_engagement = {
        **mo_engagement,
        "validity": {"from_": mo_today(), "to": mo_today()},
    }
    await assert_engagement(mo_engagement)
