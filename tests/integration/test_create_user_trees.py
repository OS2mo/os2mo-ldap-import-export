# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json
from collections.abc import Awaitable
from collections.abc import Callable
from functools import partial
from typing import Any
from uuid import UUID
from uuid import uuid4

import pytest
from ldap3 import Connection
from more_itertools import one
from more_itertools import only
from structlog.testing import capture_logs

from mo_ldap_import_export.autogenerated_graphql_client import GraphQLClient
from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    EngagementCreateInput,
)
from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    OrganisationUnitCreateInput,
)
from mo_ldap_import_export.depends import Settings
from mo_ldap_import_export.ldap import ldap_search
from mo_ldap_import_export.types import EmployeeUUID
from mo_ldap_import_export.types import OrgUnitUUID
from mo_ldap_import_export.utils import combine_dn_strings
from tests.integration.conftest import AddLdapPerson

#       root
#       / \
#      l   r
#     / \   \
#    ll lr  rr
#   /   /  /  \
# lll lrr rrl rrr
PARENT_MAP = {
    "root": None,
    "l": "root",
    "ll": "l",
    "lr": "l",
    "lll": "ll",
    "lrr": "lr",
    "r": "root",
    "rr": "r",
    "rrl": "rr",
    "rrr": "rr",
}
UUID_MAP = {name: OrgUnitUUID(uuid4()) for name in PARENT_MAP}

CONVERSION_MAPPING = json.dumps(
    {
        "mo2ldap": """
            {% set mo_employee = load_mo_employee(uuid, current_objects_only=False) %}
            {{
                {
                    "employeeNumber": mo_employee.cpr_number,
                    "uid": mo_employee.cpr_number,
                    "cn": mo_employee.given_name + " " + mo_employee.surname,
                    "sn": mo_employee.surname,
                    "givenName": mo_employee.given_name,
                    "displayName": mo_employee.nickname_given_name + " " + mo_employee.nickname_surname
                }|tojson
            }}
        """,
        "username_generator": {
            "combinations_to_try": ["FFFX", "LLLX"],
        },
    }
)


@pytest.fixture
async def fetch_ldap_account(
    ldap_connection: Connection,
    ldap_org_unit: list[str],
) -> Callable[[str], Awaitable[dict[str, Any] | None]]:
    async def inner(cpr_number: str) -> dict[str, Any] | None:
        response, _ = await ldap_search(
            ldap_connection,
            search_base=combine_dn_strings(ldap_org_unit),
            search_filter=f"(employeeNumber={cpr_number})",
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
        employee = only(response)
        if not employee:
            return None
        return employee

    return inner


@pytest.fixture
async def fetch_mo_person_ldap_account(
    fetch_ldap_account: Callable[[str], Awaitable[dict[str, Any] | None]],
) -> Callable[[], Awaitable[dict[str, Any] | None]]:
    return partial(fetch_ldap_account, "2108613133")


@pytest.fixture
async def create_engagement(
    graphql_client: GraphQLClient,
    ansat: UUID,
    jurist: UUID,
    primary: UUID,
) -> Callable[..., Awaitable[UUID]]:
    async def inner(
        person: EmployeeUUID,
        org_unit: OrgUnitUUID,
        validity: dict[str, str] | None = None,
    ) -> UUID:
        validity = validity or {"from": "1970-01-01T00:00:00"}
        result = await graphql_client.engagement_create(
            input=EngagementCreateInput(
                user_key="engagement",
                person=person,
                org_unit=org_unit,
                engagement_type=ansat,
                job_function=jurist,
                primary=primary,
                validity=validity,
            )
        )
        return result.uuid

    return inner


@pytest.fixture
async def create_org_unit(
    graphql_client: GraphQLClient, afdeling: UUID
) -> Callable[[OrgUnitUUID, OrgUnitUUID | None], Awaitable[None]]:
    async def creator(uuid: OrgUnitUUID, parent: OrgUnitUUID | None = None) -> None:
        result = await graphql_client.org_unit_create(
            OrganisationUnitCreateInput(
                uuid=uuid,
                name=str(uuid),
                user_key=str(uuid),
                parent=parent,
                org_unit_type=afdeling,
                validity={"from": "1970-01-01T00:00:00"},
            )
        )
        assert result.uuid == uuid

    return creator


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": CONVERSION_MAPPING,
    }
)
async def test_create_user_trees_not_configured(
    trigger_mo_person: Callable[[], Awaitable[None]],
    fetch_mo_person_ldap_account: Callable[[], Awaitable[dict[str, Any] | None]],
) -> None:
    settings = Settings()
    assert settings.create_user_trees == []

    with capture_logs() as cap_logs:
        await trigger_mo_person()

    log_events = [x["event"] for x in cap_logs]
    assert "create_user_trees not configured, allowing create" in log_events

    account = await fetch_mo_person_ldap_account()
    assert account is not None
    assert one(account["attributes"]["sn"]) == "Bach Klarskov"


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": CONVERSION_MAPPING,
        "CREATE_USER_TREES": json.dumps([str(UUID_MAP["root"])]),
    }
)
async def test_create_user_tree_no_engagement(
    trigger_mo_person: Callable[[], Awaitable[None]],
    fetch_mo_person_ldap_account: Callable[[], Awaitable[dict[str, Any] | None]],
) -> None:
    settings = Settings()
    assert settings.create_user_trees == [UUID_MAP["root"]]

    with capture_logs() as cap_logs:
        await trigger_mo_person()

    log_events = [x["event"] for x in cap_logs]
    assert (
        "create_user_trees configured, but no primary engagement, skipping"
        in log_events
    )

    account = await fetch_mo_person_ldap_account()
    assert account is None


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": CONVERSION_MAPPING,
        "CREATE_USER_TREES": json.dumps([str(UUID_MAP["root"])]),
    }
)
async def test_create_user_tree_past_engagement(
    trigger_mo_person: Callable[[], Awaitable[None]],
    fetch_mo_person_ldap_account: Callable[[], Awaitable[dict[str, Any] | None]],
    create_engagement: Callable[..., Awaitable[UUID]],
    mo_person: EmployeeUUID,
    mo_org_unit: OrgUnitUUID,
) -> None:
    settings = Settings()
    assert settings.create_user_trees == [UUID_MAP["root"]]

    await create_engagement(
        mo_person,
        mo_org_unit,
        validity={"from": "1970-01-01T00:00:00", "to": "1990-01-01T00:00:00"},
    )

    with capture_logs() as cap_logs:
        await trigger_mo_person()

    log_events = [x["event"] for x in cap_logs]
    assert "create_user_trees engagement is not current or future" in log_events

    account = await fetch_mo_person_ldap_account()
    assert account is None


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": CONVERSION_MAPPING,
        "CREATE_USER_TREES": json.dumps([str(UUID_MAP["root"])]),
    }
)
async def test_create_user_tree_future_engagement(
    trigger_mo_person: Callable[[], Awaitable[None]],
    fetch_mo_person_ldap_account: Callable[[], Awaitable[dict[str, Any] | None]],
    create_engagement: Callable[..., Awaitable[UUID]],
    create_org_unit: Callable[[OrgUnitUUID, OrgUnitUUID | None], Awaitable[None]],
    mo_person: EmployeeUUID,
) -> None:
    settings = Settings()
    assert settings.create_user_trees == [UUID_MAP["root"]]

    # Create engagement our engagement bound to the org_unit_target
    await create_org_unit(UUID_MAP["root"], None)
    await create_engagement(mo_person, UUID_MAP["root"])

    await create_engagement(
        mo_person,
        UUID_MAP["root"],
        validity={"from": "3000-01-01T00:00:00", "to": None},
    )

    with capture_logs() as cap_logs:
        await trigger_mo_person()

    message = "Primary engagement OU outside create_user_trees, skipping"
    log_events = [x["event"] for x in cap_logs]
    assert message not in log_events

    account = await fetch_mo_person_ldap_account()
    assert account is not None


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": CONVERSION_MAPPING,
        "CREATE_USER_TREES": json.dumps([str(UUID_MAP["root"])]),
    }
)
async def test_create_user_tree_outside_tree(
    trigger_mo_person: Callable[[], Awaitable[None]],
    fetch_mo_person_ldap_account: Callable[[], Awaitable[dict[str, Any] | None]],
    create_engagement: Callable[..., Awaitable[UUID]],
    mo_person: EmployeeUUID,
    mo_org_unit: OrgUnitUUID,
) -> None:
    settings = Settings()
    assert settings.create_user_trees == [UUID_MAP["root"]]

    await create_engagement(mo_person, mo_org_unit)

    with capture_logs() as cap_logs:
        await trigger_mo_person()

    log_events = [x["event"] for x in cap_logs]
    assert "Primary engagement OU outside create_user_trees, skipping" in log_events

    account = await fetch_mo_person_ldap_account()
    assert account is None


@pytest.mark.integration_test
@pytest.mark.usefixtures("ldap_org_unit")
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": CONVERSION_MAPPING,
        "CREATE_USER_TREES": json.dumps([str(UUID_MAP["lr"]), str(UUID_MAP["r"])]),
    }
)
@pytest.mark.parametrize(
    "org_unit_target,is_ok",
    [
        # 'root' and 'l' are not an descendents of 'lr' or 'r'
        ("root", False),
        ("l", False),
        ("ll", False),
        ("lll", False),
        # 'lr' is 'lr'
        ("lr", True),
        # 'lrr' is a descendent of 'lr'
        ("lrr", True),
        # 'r' is 'r'
        ("r", True),
        # 'rr', 'rrl' and 'rrr' are all descendents of 'r'
        ("rr", True),
        ("rrl", True),
        ("rrr", True),
    ],
)
async def test_create_user_trees_recursive_check(
    trigger_mo_person: Callable[[], Awaitable[None]],
    fetch_mo_person_ldap_account: Callable[[], Awaitable[dict[str, Any] | None]],
    create_engagement: Callable[..., Awaitable[UUID]],
    create_org_unit: Callable[[OrgUnitUUID, OrgUnitUUID | None], Awaitable[None]],
    mo_person: EmployeeUUID,
    org_unit_target: str,
    is_ok: bool,
) -> None:
    # Create org-units
    for name, parent in PARENT_MAP.items():
        own_uuid = UUID_MAP[name]
        parent_uuid = UUID_MAP.get(parent)  # type: ignore
        await create_org_unit(own_uuid, parent_uuid)

    # Create engagement our engagement bound to the org_unit_target
    await create_engagement(mo_person, UUID_MAP[org_unit_target])

    with capture_logs() as cap_logs:
        await trigger_mo_person()

    log_events = [x["event"] for x in cap_logs]
    message = "Primary engagement OU outside create_user_trees, skipping"
    if is_ok:
        assert message not in log_events
        account = await fetch_mo_person_ldap_account()
        assert account is not None
    else:
        assert message in log_events
        account = await fetch_mo_person_ldap_account()
        assert account is None


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": CONVERSION_MAPPING,
        "CREATE_USER_TREES": json.dumps([str(UUID_MAP["root"])]),
    }
)
@pytest.mark.parametrize("existing_ldap_account", [True, False])
async def test_create_user_tree_only_create(
    trigger_mo_person: Callable[[], Awaitable[None]],
    fetch_mo_person_ldap_account: Callable[[], Awaitable[dict[str, Any] | None]],
    create_engagement: Callable[..., Awaitable[UUID]],
    add_ldap_person: AddLdapPerson,
    mo_person: EmployeeUUID,
    mo_org_unit: OrgUnitUUID,
    existing_ldap_account: bool,
) -> None:
    settings = Settings()
    assert settings.create_user_trees == [UUID_MAP["root"]]

    await create_engagement(mo_person, mo_org_unit)

    # No LDAP account yet
    account = await fetch_mo_person_ldap_account()
    assert account is None

    # Create an LDAP account if configured to do so
    if existing_ldap_account:
        await add_ldap_person("abk", "2108613133")
        account = await fetch_mo_person_ldap_account()
        assert account is not None

    with capture_logs() as cap_logs:
        await trigger_mo_person()

    # create_user_trees should only be checked for creates
    # i.e. when an existing_ldap_account does not exist
    log_events = [x["event"] for x in cap_logs]
    message = "Primary engagement OU outside create_user_trees, skipping"
    if existing_ldap_account:
        assert message not in log_events
    else:
        assert message in log_events
        # If create user trees failed, we should not have gotten an account
        account = await fetch_mo_person_ldap_account()
        assert account is None
