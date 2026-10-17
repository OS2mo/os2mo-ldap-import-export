# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json
from typing import Any
from uuid import UUID
from uuid import uuid4

import pytest
from fastapi.encoders import jsonable_encoder
from fastramqpi.events import Event
from httpx import AsyncClient

from mo_ldap_import_export.exceptions import NoObjectsReturnedException
from mo_ldap_import_export.ldapapi import LDAPAPI

DRY_RUN_CONVERSION_MAPPING = {
    "mo_to_ldap": {
        "person": {
            "routing_key": "person",
            "object_class": "inetOrgPerson",
            "template": """
                {% set mo_employee = load_mo_employee(uuid) %}
                {{
                    {
                        "dn": "cn=" + mo_employee.given_name + " " + mo_employee.surname + ",ou=os2mo,o=magenta,dc=magenta,dc=dk",
                        "create": True,
                        "attributes": {
                            "employeeNumber": mo_employee.cpr_number,
                            "sn": mo_employee.surname
                        }
                    }|tojson
                }}
            """,
        }
    }
}

EXPECTED_DN = "cn=Aage Bach Klarskov,ou=os2mo,o=magenta,dc=magenta,dc=dk"


@pytest.mark.integration_test
@pytest.mark.usefixtures("ldap_org_unit")
@pytest.mark.envvar({"CONVERSION_MAPPING": json.dumps(DRY_RUN_CONVERSION_MAPPING)})
async def test_mo_to_ldap_dry_run_true(
    test_client: AsyncClient,
    ldap_api: LDAPAPI,
    mo_person: UUID,
) -> None:
    # Call the MO --> LDAP synchronization logic with dry-run = True
    response = await test_client.post(
        "/mo_to_ldap/person",
        params={"dry_run": True},
        json=jsonable_encoder(Event(subject=mo_person, priority=10)),
    )
    assert response.status_code == 451
    response_json = response.json()
    assert response_json["detail"]["message"] == "Would have created"
    dn = response_json["detail"]["dn"]
    assert dn == EXPECTED_DN

    # Ensure that no LDAP account was created
    with pytest.raises(NoObjectsReturnedException):
        await ldap_api.get_object_by_dn(dn)


@pytest.mark.integration_test
@pytest.mark.usefixtures("ldap_org_unit")
@pytest.mark.envvar({"CONVERSION_MAPPING": json.dumps(DRY_RUN_CONVERSION_MAPPING)})
@pytest.mark.parametrize("dry_run", [False, None])
async def test_mo_to_ldap_no_dry_run(
    test_client: AsyncClient,
    ldap_api: LDAPAPI,
    mo_person: UUID,
    dry_run: bool | None,
) -> None:
    # Call the MO --> LDAP synchronization logic
    response = await test_client.post(
        "/mo_to_ldap/person",
        params={"dry_run": True} if dry_run else {},
        json=jsonable_encoder(Event(subject=mo_person, priority=10)),
    )
    assert response.status_code == 200

    # Ensure that an LDAP account was created
    ldap_object = await ldap_api.get_object_by_dn(EXPECTED_DN)
    assert ldap_object is not None


# The DN of the `ldap_person` fixture
LDAP_PERSON_DN = "uid=abk,ou=os2mo,o=magenta,dc=magenta,dc=dk"


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": json.dumps(
            {
                "mo_to_ldap": {
                    "create": {
                        "routing_key": "person",
                        "object_class": "inetOrgPerson",
                        "template": """
                            {{
                                {
                                    "dn": "cn=Aage Bach Klarskov,ou=os2mo,o=magenta,dc=magenta,dc=dk",
                                    "create": True,
                                    "attributes": {"employeeNumber": "2108613133", "sn": "Bach Klarskov"}
                                }|tojson
                            }}
                        """,
                    },
                    "modify": {
                        "routing_key": "person",
                        "object_class": "inetOrgPerson",
                        "template": """
                            {{
                                {
                                    "dn": "uid=abk,ou=os2mo,o=magenta,dc=magenta,dc=dk",
                                    "create": False,
                                    "attributes": {"title": "Skoleleder"}
                                }|tojson
                            }}
                        """,
                    },
                    "modify_dn": {
                        "routing_key": "person",
                        "object_class": "inetOrgPerson",
                        "template": """
                            {{
                                {
                                    "dn": "uid=abk,ou=os2mo,o=magenta,dc=magenta,dc=dk",
                                    "create": False,
                                    "attributes": {"uid": "aage"}
                                }|tojson
                            }}
                        """,
                    },
                    "no_changes": {
                        "routing_key": "person",
                        "object_class": "inetOrgPerson",
                        "template": """
                            {{
                                {
                                    "dn": "uid=abk,ou=os2mo,o=magenta,dc=magenta,dc=dk",
                                    "create": False,
                                    "attributes": {"title": "Skole underviser"}
                                }|tojson
                            }}
                        """,
                    },
                    "skip": {
                        "routing_key": "person",
                        "object_class": "inetOrgPerson",
                        "template": "{{ skip_if_none(None) }}",
                    },
                }
            }
        ),
    }
)
@pytest.mark.usefixtures("ldap_person")
@pytest.mark.parametrize(
    "identifier,status_code,expected",
    [
        (
            "create",
            451,
            {
                "detail": {
                    "message": "Would have created",
                    "dn": EXPECTED_DN,
                    "object_class": "inetOrgPerson",
                    "attributes": {
                        "employeeNumber": ["2108613133"],
                        "sn": ["Bach Klarskov"],
                    },
                }
            },
        ),
        (
            "modify",
            451,
            {
                "detail": {
                    "message": "Would have changed attributes",
                    "dn": LDAP_PERSON_DN,
                    "attributes": {"title": ["Skoleleder"]},
                    "old_attributes": {"title": ["Skole underviser"]},
                }
            },
        ),
        (
            "modify_dn",
            451,
            {
                "detail": {
                    "message": "Would have changed DN",
                    "dn": LDAP_PERSON_DN,
                    "new_rdn": "uid=aage",
                }
            },
        ),
        ("no_changes", 200, None),
        ("skip", 200, None),
    ],
)
async def test_mo_to_ldap_dry_run_output(
    test_client: AsyncClient,
    ldap_api: LDAPAPI,
    identifier: str,
    status_code: int,
    expected: dict[str, Any] | None,
) -> None:
    ldap_person = await ldap_api.get_object_by_dn(LDAP_PERSON_DN)

    response = await test_client.post(
        f"/mo_to_ldap/{identifier}",
        params={"dry_run": True},
        json=jsonable_encoder(Event(subject=uuid4(), priority=10)),
    )
    assert response.status_code == status_code
    assert response.json() == expected

    # Ensure that nothing was written to LDAP
    assert await ldap_api.get_object_by_dn(LDAP_PERSON_DN) == ldap_person
    with pytest.raises(NoObjectsReturnedException):
        await ldap_api.get_object_by_dn(EXPECTED_DN)


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "LDAP_READ_ONLY": "True",
        "CONVERSION_MAPPING": json.dumps(
            {
                "mo_to_ldap": {
                    "create": {
                        "routing_key": "person",
                        "object_class": "inetOrgPerson",
                        "template": """
                            {{
                                {
                                    "dn": "cn=Aage Bach Klarskov,ou=os2mo,o=magenta,dc=magenta,dc=dk",
                                    "create": True,
                                    "attributes": {"employeeNumber": "2108613133", "sn": "Bach Klarskov"}
                                }|tojson
                            }}
                        """,
                    },
                    "modify": {
                        "routing_key": "person",
                        "object_class": "inetOrgPerson",
                        "template": """
                            {{
                                {
                                    "dn": "uid=abk,ou=os2mo,o=magenta,dc=magenta,dc=dk",
                                    "create": False,
                                    "attributes": {"title": "Skoleleder"}
                                }|tojson
                            }}
                        """,
                    },
                    "modify_dn": {
                        "routing_key": "person",
                        "object_class": "inetOrgPerson",
                        "template": """
                            {{
                                {
                                    "dn": "uid=abk,ou=os2mo,o=magenta,dc=magenta,dc=dk",
                                    "create": False,
                                    "attributes": {"uid": "aage"}
                                }|tojson
                            }}
                        """,
                    },
                    "no_changes": {
                        "routing_key": "person",
                        "object_class": "inetOrgPerson",
                        "template": """
                            {{
                                {
                                    "dn": "uid=abk,ou=os2mo,o=magenta,dc=magenta,dc=dk",
                                    "create": False,
                                    "attributes": {"title": "Skole underviser"}
                                }|tojson
                            }}
                        """,
                    },
                    "skip": {
                        "routing_key": "person",
                        "object_class": "inetOrgPerson",
                        "template": "{{ skip_if_none(None) }}",
                    },
                }
            }
        ),
    }
)
@pytest.mark.usefixtures("ldap_person")
@pytest.mark.parametrize(
    "identifier,expected",
    [
        (
            "create",
            {
                "detail": {
                    "message": "LDAP connection is read-only",
                    "dn": EXPECTED_DN,
                    "requested_state": {
                        "employeeNumber": ["2108613133"],
                        "sn": ["Bach Klarskov"],
                    },
                    "old_state": None,
                }
            },
        ),
        (
            "modify",
            {
                "detail": {
                    "message": "LDAP connection is read-only",
                    "dn": LDAP_PERSON_DN,
                    "requested_state": {"title": [["MODIFY_REPLACE", ["Skoleleder"]]]},
                    "old_state": None,
                }
            },
        ),
        (
            "modify_dn",
            {
                "detail": {
                    "message": "LDAP connection is read-only",
                    "dn": LDAP_PERSON_DN,
                    "requested_state": {},
                    "old_state": None,
                }
            },
        ),
        ("no_changes", None),
        ("skip", None),
    ],
)
async def test_mo_to_ldap_read_only_output(
    test_client: AsyncClient,
    ldap_api: LDAPAPI,
    identifier: str,
    expected: dict[str, Any] | None,
) -> None:
    ldap_person = await ldap_api.get_object_by_dn(LDAP_PERSON_DN)

    response = await test_client.post(
        f"/mo_to_ldap/{identifier}",
        json=jsonable_encoder(Event(subject=uuid4(), priority=10)),
    )
    # Read-only acknowledges the event, as retrying would not change anything
    assert response.status_code == 200
    assert response.json() == expected

    # Ensure that nothing was written to LDAP
    assert await ldap_api.get_object_by_dn(LDAP_PERSON_DN) == ldap_person
    with pytest.raises(NoObjectsReturnedException):
        await ldap_api.get_object_by_dn(EXPECTED_DN)
