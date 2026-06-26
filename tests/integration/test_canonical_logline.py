# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import MutableMapping
from typing import Any

import pytest
import structlog
from httpx import AsyncClient
from more_itertools import one
from structlog.contextvars import merge_contextvars
from structlog.testing import capture_logs

from mo_ldap_import_export.types import LDAPUUID
from mo_ldap_import_export.types import EmployeeUUID


@pytest.fixture
def capture_logs_with_merged_contextvars(
    test_client: AsyncClient,
) -> Iterator[list[MutableMapping[str, Any]]]:
    """Capture logs, re-adding the merge_contextvars that capture_logs strips."""
    with capture_logs() as logs:
        structlog.get_config()["processors"].insert(0, merge_contextvars)
        yield logs


def canonical_logline(logs: list[MutableMapping[str, Any]]) -> MutableMapping[str, Any]:
    """Return the single canonical log line emitted for the request."""
    return one(log for log in logs if log["event"] == "Request handled")


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
                            "givenName": mo_employee.given_name,
                            "sn": mo_employee.surname,
                            "cn": mo_employee.given_name ~ " " ~ mo_employee.surname
                        }|tojson
                    }}
                """,
            }
        ),
    }
)
@pytest.mark.usefixtures("ldap_org_unit")
async def test_canonical_logline_mo_to_ldap(
    capture_logs_with_merged_contextvars: list[MutableMapping[str, Any]],
    trigger_mo_person: Callable[[], Awaitable[None]],
    mo_person: EmployeeUUID,
) -> None:
    await trigger_mo_person()

    line = canonical_logline(capture_logs_with_merged_contextvars)
    assert line["subject"] == str(mo_person)
    assert line["duration"] >= 0
    assert "request_id" in line


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
                        "_import_to_mo_": "true",
                        "_ldap_attributes_": ["employeeNumber", "givenName", "sn"],
                        "uuid": "{{ employee_uuid }}",
                        "cpr_number": "{{ ldap.employeeNumber }}",
                        "given_name": "{{ ldap.givenName }}",
                        "surname": "{{ ldap.sn }}",
                    },
                },
            }
        ),
    }
)
@pytest.mark.usefixtures("mo_org_unit")
async def test_canonical_logline_ldap_to_mo(
    capture_logs_with_merged_contextvars: list[MutableMapping[str, Any]],
    trigger_ldap_person: Callable[[], Awaitable[None]],
    ldap_person_uuid: LDAPUUID,
) -> None:
    await trigger_ldap_person()

    line = canonical_logline(capture_logs_with_merged_contextvars)
    assert line["subject"] == str(ldap_person_uuid)
    assert line["duration"] >= 0
    assert "request_id" in line
