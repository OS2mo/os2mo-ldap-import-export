# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json
from collections.abc import AsyncIterator
from collections.abc import Iterable
from typing import Any
from unittest.mock import ANY
from unittest.mock import AsyncMock
from unittest.mock import patch
from uuid import uuid4

import pytest
from fastramqpi.ramqp.depends import Context
from fastramqpi.ramqp.utils import RequeueMessage
from ldap3 import BASE
from ldap3 import MOCK_ASYNC
from ldap3 import SUBTREE
from ldap3 import Connection
from more_itertools import one
from pydantic import parse_obj_as
from structlog.testing import capture_logs

from mo_ldap_import_export.config import Settings
from mo_ldap_import_export.config import UsernameGeneratorConfig
from mo_ldap_import_export.converters import LdapConverter
from mo_ldap_import_export.customer_specific_checks import ExportChecks
from mo_ldap_import_export.customer_specific_checks import ImportChecks
from mo_ldap_import_export.dataloaders import DataLoader
from mo_ldap_import_export.depends import GraphQLClient
from mo_ldap_import_export.import_export import SyncTool
from mo_ldap_import_export.ldap import apply_discriminator
from mo_ldap_import_export.ldap import configure_ldap_connection
from mo_ldap_import_export.ldap import construct_server_pool
from mo_ldap_import_export.ldap import get_ldap_object
from mo_ldap_import_export.ldap import wait_for_message_id
from mo_ldap_import_export.ldap_classes import LdapObject
from mo_ldap_import_export.routes import load_ldap_OUs
from mo_ldap_import_export.types import DN
from mo_ldap_import_export.usernames import UserNameGenerator
from mo_ldap_import_export.utils import extract_ou_from_dn
from tests.graphql_mocker import GraphQLMocker


@pytest.fixture
async def graphql_client() -> AsyncIterator[GraphQLClient]:
    # NOTE: We could have this session-scoped as it is essentially stateless
    async with GraphQLClient("http://example.com/graphql") as graphql_client:
        yield graphql_client


@pytest.fixture
def settings(
    minimal_valid_environmental_variables: None,
    monkeypatch: pytest.MonkeyPatch,
) -> Settings:
    monkeypatch.setenv(
        "CONVERSION_MAPPING",
        json.dumps(
            {
                "ldap_to_mo": {
                    "Employee": {
                        "objectClass": "ramodels.mo.employee.Employee",
                        "_import_to_mo_": "false",
                        "_ldap_attributes_": ["employeeID"],
                        "cpr_number": "{{ldap.employeeID or None}}",
                        "uuid": "{{ employee_uuid or NONE }}",
                    }
                },
                "mo2ldap": """
                {}
                """,
                "username_generator": {"objectClass": "UserNameGenerator"},
            }
        ),
    )

    return Settings()


@pytest.fixture
def ldap_container_dn() -> str:
    return "o=example"


@pytest.fixture
def ldap_connection(settings: Settings, ldap_container_dn: str) -> Iterable[Connection]:
    """Fixture to construct a mocked ldap_connection.

    Returns:
        The mocked configured ldap_connection.
    """
    # See https://ldap3.readthedocs.io/en/latest/mocking.html for details
    with patch(
        "mo_ldap_import_export.ldap.get_client_strategy", return_value=MOCK_ASYNC
    ):
        # This patch is necessary due to: https://github.com/cannatag/ldap3/issues/1007
        server = one(construct_server_pool(settings).servers)
        with patch(
            "mo_ldap_import_export.ldap.construct_server_pool", return_value=server
        ):
            entryUUID = uuid4()

            ldap_connection = configure_ldap_connection(settings)
            ldap_connection.strategy.add_entry(
                f"CN={settings.ldap_user},{ldap_container_dn}",
                {
                    "objectClass": "inetOrgPerson",
                    "userPassword": settings.ldap_password.get_secret_value(),
                    "sn": f"{settings.ldap_user}_sn",
                    "revision": 0,
                    "entryUUID": "{" + str(entryUUID) + "}",
                    "employeeID": "0101700001",
                },
            )
            ldap_connection.bind()
            yield ldap_connection


@pytest.fixture
def ldap_dn(settings: Settings, ldap_container_dn: str) -> DN:
    return DN(f"CN={settings.ldap_user},{ldap_container_dn}")


async def test_searching_mocked(
    ldap_connection: Connection, settings: Settings, ldap_container_dn: str
) -> None:
    """Test that we can use the mocked ldap_connection to search for our default user."""
    message_id = ldap_connection.search(
        ldap_container_dn,
        f"(cn={settings.ldap_user})",
        search_scope=SUBTREE,
        attributes="*",
    )
    response, result = await wait_for_message_id(ldap_connection, message_id)
    assert result["description"] == "success"
    assert response is not None
    search_result = one(response)
    assert search_result == {
        "attributes": {
            "objectClass": ["inetOrgPerson"],
            "userPassword": [settings.ldap_password.get_secret_value()],
            "sn": [f"{settings.ldap_user}_sn"],
            "revision": ["0"],
            "CN": [settings.ldap_user],
            "entryUUID": ANY,
            "employeeID": ["0101700001"],
        },
        "dn": f"CN={settings.ldap_user},{ldap_container_dn}",
        "raw_attributes": ANY,
        "raw_dn": ANY,
        "type": "searchResEntry",
    }


async def test_searching_newly_added(ldap_connection: Connection) -> None:
    """Test that we can use the mocked ldap_connection to find newly added users."""
    username = str(uuid4())
    password = str(uuid4())
    container = str(uuid4())
    entryUUID = str(uuid4())
    # Add new entry
    ldap_connection.strategy.add_entry(
        f"cn={username},o={container}",
        {
            "objectClass": "inetOrgPerson",
            "userPassword": password,
            "sn": f"{username}_sn",
            "revision": 1,
            "entryUUID": "{" + entryUUID + "}",
            "employeeID": "0101700002",
        },
    )

    message_id = ldap_connection.search(
        f"o={container}", f"(cn={username})", search_scope=SUBTREE, attributes="*"
    )
    response, result = await wait_for_message_id(ldap_connection, message_id)
    assert result["description"] == "success"
    assert response is not None
    search_result = one(response)
    assert search_result == {
        "attributes": {
            "objectClass": ["inetOrgPerson"],
            "userPassword": [password],
            "sn": [f"{username}_sn"],
            "revision": ["1"],
            "CN": [username],
            "employeeID": ["0101700002"],
            "entryUUID": ANY,
        },
        "dn": f"cn={username},o={container}",
        "raw_attributes": ANY,
        "raw_dn": ANY,
        "type": "searchResEntry",
    }


async def test_searching_dn_lookup(
    ldap_connection: Connection, settings: Settings, ldap_dn: DN, ldap_container_dn: str
) -> None:
    """Test that we can read our default user."""
    message_id = ldap_connection.search(
        ldap_dn,
        "(objectclass=*)",
        attributes="*",
        search_scope=BASE,
    )
    response, result = await wait_for_message_id(ldap_connection, message_id)
    assert result["description"] == "success"
    assert response is not None
    search_result = one(response)
    assert search_result == {
        "attributes": {
            "objectClass": ["inetOrgPerson"],
            "userPassword": [settings.ldap_password.get_secret_value()],
            "sn": [f"{settings.ldap_user}_sn"],
            "revision": ["0"],
            "CN": [settings.ldap_user],
            "entryUUID": ANY,
            "employeeID": ["0101700001"],
        },
        "dn": f"CN={settings.ldap_user},{ldap_container_dn}",
        "raw_attributes": ANY,
        "raw_dn": ANY,
        "type": "searchResEntry",
    }


@pytest.mark.parametrize(
    "attributes,expected",
    [
        # Reading 'None' reads all fields
        (
            None,
            {
                "CN": ["foo"],
                "objectClass": ["inetOrgPerson"],
                "revision": ["0"],
                "sn": ["foo_sn"],
                "userPassword": ["foo"],
                "employeeID": ["0101700001"],
                "entryUUID": ANY,
            },
        ),
        # Reading no fields reads dn
        ([], {}),
        # Read SN
        (["sn"], {"sn": ["foo_sn"]}),
        (["SN"], {"sn": ["foo_sn"]}),
        # Read CN
        (["cn"], {"CN": ["foo"]}),
        (["CN"], {"CN": ["foo"]}),
        # Read SN and CN
        (["sn", "cn"], {"sn": ["foo_sn"], "CN": ["foo"]}),
        (["sn", "CN"], {"sn": ["foo_sn"], "CN": ["foo"]}),
        # Read unknown field
        (["__invalid__"], {"__invalid__": []}),
    ],
)
async def test_get_ldap_object(
    ldap_connection: Connection,
    ldap_dn: DN,
    attributes: list[str],
    expected: dict[str, Any],
) -> None:
    """Test that get_ldap_object can read specific attributes on our default user."""
    result = await get_ldap_object(ldap_connection, ldap_dn, attributes=attributes)
    assert result.dn == ldap_dn
    assert result.__dict__ == {"dn": "CN=foo,o=example"} | expected


async def test_get_ldap_cpr_object(
    ldap_connection: Connection,
    settings: Settings,
    ldap_container_dn: str,
) -> None:
    message_id = ldap_connection.search(
        ldap_container_dn,
        "(&(objectclass=inetOrgPerson)(employeeID=0101700001))",
        search_scope=SUBTREE,
        attributes="*",
    )
    response, result = await wait_for_message_id(ldap_connection, message_id)
    assert result["description"] == "success"
    assert response is not None
    search_result = one(response)
    assert search_result == {
        "attributes": {
            "objectClass": ["inetOrgPerson"],
            "userPassword": [settings.ldap_password.get_secret_value()],
            "sn": [f"{settings.ldap_user}_sn"],
            "revision": ["0"],
            "CN": [settings.ldap_user],
            "entryUUID": ANY,
            "employeeID": ["0101700001"],
        },
        "dn": f"CN={settings.ldap_user},{ldap_container_dn}",
        "raw_attributes": ANY,
        "raw_dn": ANY,
        "type": "searchResEntry",
    }


async def test_apply_discriminator_no_config(
    ldap_connection: Connection, settings: Settings
) -> None:
    """Test that apply_discriminator only allows one DN when not configured."""
    assert settings.discriminator_field is None

    result = await apply_discriminator(settings, ldap_connection, set())
    assert result is None

    result = await apply_discriminator(settings, ldap_connection, {"CN=Anzu"})
    assert result == "CN=Anzu"

    with pytest.raises(ValueError) as exc_info:
        await apply_discriminator(settings, ldap_connection, {"CN=Anzu", "CN=Arak"})
    assert "Expected exactly one item in iterable" in str(exc_info.value)


@pytest.mark.parametrize(
    "discriminator_settings",
    [
        # Needs function and values
        {
            "discriminator_field": "sn",
        },
        # Needs function
        {
            "discriminator_field": "sn",
            "discriminator_values": ["__never_gonna_match__"],
        },
        # Needs values
        {"discriminator_field": "sn", "discriminator_function": "exclude"},
        # Cannot give empty values
        {
            "discriminator_field": "sn",
            "discriminator_function": "exclude",
            "discriminator_values": [],
        },
        # Cannot give invalid function
        {
            "discriminator_field": "sn",
            "discriminator_function": "__invalid__",
            "discriminator_values": ["__never_gonna_match__"],
        },
    ],
)
async def test_apply_discriminator_settings_invariants(
    ldap_connection: Connection,
    settings: Settings,
    ldap_dn: DN,
    discriminator_settings: dict[str, Any],
) -> None:
    """Test that apply_discriminator checks settings invariants."""
    with pytest.raises(AssertionError):
        # Need function and values
        new_settings = settings.copy(update=discriminator_settings)
        await apply_discriminator(new_settings, ldap_connection, {ldap_dn})


async def test_apply_discriminator_unknown_dn(
    ldap_connection: Connection, settings: Settings
) -> None:
    """Test that apply_discriminator requeues on missing DNs."""
    settings = settings.copy(
        update={
            "discriminator_field": "sn",
            "discriminator_function": "exclude",
            "discriminator_values": ["__never_gonna_match__"],
        }
    )
    with pytest.raises(RequeueMessage) as exc_info:
        await apply_discriminator(settings, ldap_connection, {"CN=__missing__dn__"})
    assert "Unable to lookup DN(s)" in str(exc_info.value)


@pytest.mark.parametrize(
    "discriminator_values,matches",
    [
        # These do not contain foo_sn
        ([""], False),
        (["__never_gonna_match__"], False),
        (["__never_gonna_match__", "bar_sn"], False),
        (["bar_sn", "__never_gonna_match__"], False),
        # These contain foo_sn
        (["foo_sn"], True),
        (["__never_gonna_match__", "foo_sn"], True),
        (["foo_sn", "__never_gonna_match__"], True),
    ],
)
@pytest.mark.parametrize("discriminator_function", ("include", "exclude"))
async def test_apply_discriminator_exclude_one_user(
    ldap_connection: Connection,
    settings: Settings,
    ldap_dn: DN,
    discriminator_function: str,
    discriminator_values: list[str],
    matches: bool,
) -> None:
    """Test that apply_discriminator exclude works with a single user on valid settings."""
    # This DN has 'foo_sn' as their sn
    if discriminator_function == "include":
        expected = ldap_dn if matches else None
    else:
        expected = None if matches else ldap_dn

    settings = settings.copy(
        update={
            "discriminator_field": "sn",
            "discriminator_function": discriminator_function,
            "discriminator_values": discriminator_values,
        }
    )
    result = await apply_discriminator(settings, ldap_connection, {ldap_dn})
    assert result == expected


@pytest.mark.parametrize("discriminator_function", ("include", "exclude"))
async def test_apply_discriminator_exclude_none(
    ldap_connection: Connection,
    settings: Settings,
    discriminator_function: str,
    ldap_container_dn: str,
) -> None:
    """Test that apply_discriminator exclude works with a single user on valid settings."""
    another_username = "bar"
    another_ldap_dn = f"CN={another_username},{ldap_container_dn}"
    ldap_connection.strategy.add_entry(
        another_ldap_dn,
        {
            "objectClass": "inetOrgPerson",
            "userPassword": str(uuid4()),
            "sn": [],
            "revision": 1,
            "entryUUID": "{" + str(uuid4()) + "}",
            "employeeID": "0101700001",
        },
    )

    settings = settings.copy(
        update={
            "discriminator_field": "sn",
            "discriminator_function": discriminator_function,
            "discriminator_values": ["foo_sn"],
        }
    )
    with capture_logs() as cap_logs:
        result = await apply_discriminator(settings, ldap_connection, {another_ldap_dn})
        assert "Discriminator value is None" in (x["event"] for x in cap_logs)

    if discriminator_function == "include":
        assert result is None
    else:
        assert result == another_ldap_dn


@pytest.mark.parametrize("discriminator_function", ("include", "exclude"))
async def test_apply_discriminator_missing_field(
    ldap_connection: Connection,
    settings: Settings,
    discriminator_function: str,
    ldap_container_dn: str,
) -> None:
    """Test that apply_discriminator exclude works with a single user on valid settings."""
    another_username = "bar"
    another_ldap_dn = f"CN={another_username},{ldap_container_dn}"
    ldap_connection.strategy.add_entry(
        another_ldap_dn,
        {
            "objectClass": "inetOrgPerson",
            "userPassword": str(uuid4()),
            "revision": 1,
            "entryUUID": "{" + str(uuid4()) + "}",
            "employeeID": "0101700001",
        },
    )
    settings = settings.copy(
        update={
            "discriminator_field": "hkOS2MOSync",
            "discriminator_function": discriminator_function,
            "discriminator_values": ["No"],
        }
    )
    with capture_logs() as cap_logs:
        result = await apply_discriminator(settings, ldap_connection, {another_ldap_dn})
        assert "Discriminator value is None" in (x["event"] for x in cap_logs)

    if discriminator_function == "include":
        assert result is None
    else:
        assert result == another_ldap_dn


@pytest.fixture
async def sync_tool_and_context(
    ldap_connection: Connection,
    ldap_container_dn: str,
    settings: Settings,
    graphql_client: GraphQLClient,
    graphql_mock: GraphQLMocker,
) -> tuple[SyncTool, Context]:
    settings = settings.copy(
        update={
            "ldap_unique_id_field": "entryUUID",
            "ldap_search_base": ldap_container_dn,
        }
    )

    route = graphql_mock.query("read_facet_classes")
    route.result = {"classes": {"objects": []}}

    route = graphql_mock.query("read_itsystems")
    route.result = {"itsystems": {"objects": []}}

    route = graphql_mock.query("read_org_units")
    route.result = {"org_units": {"objects": []}}

    route = graphql_mock.query("read_class_user_keys")
    route.result = {"classes": {"objects": []}}

    amqpsystem = AsyncMock()
    context: Context = {
        "user_context": {
            "ldap_connection": ldap_connection,
            "settings": settings,
        },
        "graphql_client": graphql_client,
        "amqpsystem": amqpsystem,
    }
    # Needs context, user_context, ldap_connection
    dataloader = DataLoader(context, amqpsystem)
    context["user_context"]["dataloader"] = dataloader

    # Needs context, user_context, settings, raw_mapping, dataloader
    converter = LdapConverter(settings, dataloader)
    context["user_context"]["converter"] = converter

    username_generator = UserNameGenerator(
        settings,
        settings.conversion_mapping.username_generator,
        dataloader,
        ldap_connection,
    )
    context["user_context"]["username_generator"] = username_generator

    export_checks = ExportChecks(dataloader)
    import_checks = ImportChecks()

    sync_tool = SyncTool(
        dataloader, converter, export_checks, import_checks, settings, ldap_connection
    )
    context["user_context"]["synctool"] = sync_tool

    return sync_tool, context


@pytest.fixture
async def sync_tool(sync_tool_and_context: tuple[SyncTool, Context]) -> SyncTool:
    return sync_tool_and_context[0]


@pytest.fixture
async def context(sync_tool_and_context: tuple[SyncTool, Context]) -> Context:
    return sync_tool_and_context[1]


@pytest.mark.parametrize(
    "extra_account,log_lines",
    [
        # Discriminator not configured
        pytest.param(
            False,
            [
                "Import to MO filtered",
                "Import checks executed",
            ],
            marks=pytest.mark.envvar({}),
        ),
        # Discriminator rejecting all accounts
        pytest.param(
            True,
            [
                "Found DN",
                "Found DN",
                "Aborting synchronization, as no good LDAP account was found",
            ],
            marks=pytest.mark.envvar(
                {
                    "DISCRIMINATOR_FIELD": "sn",
                    "DISCRIMINATOR_FUNCTION": "include",
                    "DISCRIMINATOR_VALUES": '["__never_gonna_match__"]',
                }
            ),
        ),
        # Discriminator finding original account
        pytest.param(
            True,
            [
                "Found DN",
                "Found DN",
                "Import to MO filtered",
                "Import checks executed",
            ],
            marks=pytest.mark.envvar(
                {
                    "DISCRIMINATOR_FIELD": "sn",
                    "DISCRIMINATOR_FUNCTION": "include",
                    "DISCRIMINATOR_VALUES": '["foo_sn"]',
                }
            ),
        ),
        # Discriminator finding another account
        pytest.param(
            True,
            [
                "Found DN",
                "Found DN",
                "Found better DN for employee",
                "Import to MO filtered",
                "Import checks executed",
            ],
            marks=pytest.mark.envvar(
                {
                    "DISCRIMINATOR_FIELD": "sn",
                    "DISCRIMINATOR_FUNCTION": "include",
                    "DISCRIMINATOR_VALUES": '["bar_sn"]',
                }
            ),
        ),
    ],
)
@pytest.mark.envvar(
    {
        "CONVERSION_MAPPING": json.dumps(
            {
                "ldap_to_mo": {
                    "Employee": {
                        "objectClass": "ramodels.mo.employee.Employee",
                        "_import_to_mo_": "false",
                        "_ldap_attributes_": ["employeeID"],
                        "cpr_number": "{{ldap.employeeID or None}}",
                        "uuid": "{{ employee_uuid or NONE }}",
                    }
                },
                "username_generator": {"objectClass": "UserNameGenerator"},
            }
        )
    }
)
async def test_import_single_user_apply_discriminator(
    ldap_connection: Connection,
    ldap_container_dn: str,
    ldap_dn: DN,
    graphql_mock: GraphQLMocker,
    sync_tool: SyncTool,
    extra_account: bool,
    log_lines: list[str],
) -> None:
    if extra_account:
        another_username = "bar"
        ldap_connection.strategy.add_entry(
            f"CN={another_username},{ldap_container_dn}",
            {
                "objectClass": "inetOrgPerson",
                "userPassword": str(uuid4()),
                "sn": f"{another_username}_sn",
                "revision": 1,
                "entryUUID": "{" + str(uuid4()) + "}",
                "employeeID": "0101700001",
            },
        )

    route = graphql_mock.query("read_employee_uuid_by_ituser_user_key")
    route.result = {"itusers": {"objects": []}}

    employee_uuid = uuid4()

    route = graphql_mock.query("read_employee_uuid_by_cpr_number")
    route.result = {"employees": {"objects": [{"uuid": employee_uuid}]}}

    route = graphql_mock.query("read_employees")
    route.result = {
        "employees": {
            "objects": [
                {
                    "validities": [
                        {
                            "uuid": employee_uuid,
                            "cpr_number": "0101700001",
                            "given_name": "Chen",
                            "surname": "Stormstout",
                            "nickname_given_name": "Chen",
                            "nickname_surname": "Brewmaster",
                            "validity": {"from": "1970-01-01T00:00:00", "to": None},
                        }
                    ]
                }
            ]
        }
    }

    with capture_logs() as cap_logs:
        await sync_tool.import_single_user(ldap_dn)
    events = [x["event"] for x in cap_logs if x["log_level"] != "debug"]

    assert (
        events
        == [
            "Generating DN",
            "Importing user",
            "Found DN",
            "Found employee via CPR matching",
            "Attempting to find DNs",
            "Attempting CPR number lookup",
            "Found LDAP(s) object",
            "Found DN(s) using CPR number lookup",
        ]
        + log_lines
    )


@pytest.mark.parametrize(
    "extra_account,log_lines",
    [
        # Discriminator not configured
        pytest.param(
            False,
            [
                "Not writing to LDAP as changeset is empty",
            ],
            marks=pytest.mark.envvar({}),
        ),
        # Discriminator rejecting all accounts
        pytest.param(
            True,
            [
                "Found DN",
                "Found DN",
                "Aborting synchronization, as no good LDAP account was found",
            ],
            marks=pytest.mark.envvar(
                {
                    "DISCRIMINATOR_FIELD": "sn",
                    "DISCRIMINATOR_FUNCTION": "include",
                    "DISCRIMINATOR_VALUES": '["__never_gonna_match__"]',
                }
            ),
        ),
        # Discriminator finding original account
        pytest.param(
            True,
            [
                "Found DN",
                "Found DN",
                "Not writing to LDAP as changeset is empty",
            ],
            marks=pytest.mark.envvar(
                {
                    "DISCRIMINATOR_FIELD": "sn",
                    "DISCRIMINATOR_FUNCTION": "include",
                    "DISCRIMINATOR_VALUES": '["foo_sn"]',
                }
            ),
        ),
        # Discriminator finding another account
        pytest.param(
            True,
            [
                "Found DN",
                "Found DN",
                "Not writing to LDAP as changeset is empty",
            ],
            marks=pytest.mark.envvar(
                {
                    "DISCRIMINATOR_FIELD": "sn",
                    "DISCRIMINATOR_FUNCTION": "include",
                    "DISCRIMINATOR_VALUES": '["bar_sn"]',
                }
            ),
        ),
    ],
)
async def test_listen_to_changes_in_employees(
    ldap_connection: Connection,
    ldap_container_dn: str,
    ldap_dn: DN,
    graphql_mock: GraphQLMocker,
    sync_tool: SyncTool,
    extra_account: bool,
    log_lines: list[str],
) -> None:
    if extra_account:
        another_username = "bar"
        ldap_connection.strategy.add_entry(
            f"CN={another_username},{ldap_container_dn}",
            {
                "objectClass": "inetOrgPerson",
                "userPassword": str(uuid4()),
                "sn": f"{another_username}_sn",
                "revision": 1,
                "entryUUID": "{" + str(uuid4()) + "}",
                "employeeID": "0101700001",
            },
        )

    route = graphql_mock.query("read_employee_uuid_by_ituser_user_key")
    route.result = {"itusers": {"objects": []}}

    employee_uuid = uuid4()

    route = graphql_mock.query("read_employee_uuid_by_cpr_number")
    route.result = {"employees": {"objects": [{"uuid": employee_uuid}]}}

    route = graphql_mock.query("read_employees")
    route.result = {
        "employees": {
            "objects": [
                {
                    "validities": [
                        {
                            "uuid": employee_uuid,
                            "cpr_number": "0101700001",
                            "given_name": "Chen",
                            "surname": "Stormstout",
                            "nickname_given_name": "Chen",
                            "nickname_surname": "Brewmaster",
                            "validity": {"from": "1970-01-01T00:00:00", "to": None},
                        }
                    ]
                }
            ]
        }
    }

    with capture_logs() as cap_logs:
        await sync_tool.listen_to_changes_in_employees(employee_uuid)
    events = [x["event"] for x in cap_logs if x["log_level"] != "debug"]

    assert (
        events
        == [
            "Registered change in an employee",
            "Attempting to find DNs",
            "Attempting CPR number lookup",
            "Found LDAP(s) object",
            "Found DN(s) using CPR number lookup",
            "Found DNs for user",
        ]
        + log_lines
    )


@pytest.mark.parametrize(
    "field,dn_map,template,expected",
    [
        # Check no template matches
        ("dn", {"CN=foo": {}, "CN=bar": {}}, "{{ False }}", None),
        ("dn", {"CN=foo": {}, "CN=bar": {}}, "{{ PleaseHelpMe }}", None),
        # Check dn is specific value
        ("dn", {"CN=foo": {}, "CN=bar": {}}, "{{ dn == 'CN=foo'}}", "CN=foo"),
        ("dn", {"CN=foo": {}, "CN=bar": {}}, "{{ dn == 'CN=bar' }}", "CN=bar"),
        # Check SN value
        (
            "sn",
            {"CN=foo": {"sn": "foo"}, "CN=bar": {"sn": "bar"}},
            "{{ value == 'foo'}}",
            "CN=foo",
        ),
        (
            "sn",
            {"CN=foo": {"sn": "foo"}, "CN=bar": {"sn": "bar"}},
            "{{ value == 'bar' }}",
            "CN=bar",
        ),
        # Check SN substring
        (
            "sn",
            {"CN=foo": {"sn": "something foo maybe"}, "CN=bar": {"sn": "bar"}},
            "{{ 'foo' in value }}",
            "CN=foo",
        ),
        # Check SN even
        (
            "sn",
            {"CN=foo": {"sn": "1"}, "CN=bar": {"sn": "3"}, "CN=baz": {"sn": "0"}},
            "{{ value|int % 2 == 0 }}",
            "CN=baz",
        ),
    ],
)
async def test_apply_discriminator_template(
    settings: Settings,
    field: str,
    dn_map: dict[DN, dict[str, Any]],
    template: str,
    expected: DN | None,
) -> None:
    settings = settings.copy(
        update={
            "discriminator_field": field,
            "discriminator_function": "template",
            "discriminator_values": [template],
        }
    )
    ldap_connection = AsyncMock()

    async def get_ldap_object(
        ldap_connection: Connection, dn: DN, *args: Any, **kwargs: Any
    ) -> LdapObject:
        return parse_obj_as(LdapObject, {"dn": dn, **dn_map[dn]})

    with patch("mo_ldap_import_export.ldap.get_ldap_object", wraps=get_ldap_object):
        result = await apply_discriminator(
            settings, ldap_connection, set(dn_map.keys())
        )
        assert result == expected


async def test_get_existing_values(sync_tool: SyncTool, context: Context) -> None:
    mapping = {
        "username_generator": {
            "objectClass": "UserNameGenerator",
        },
    }

    user_context = context["user_context"]
    username_generator = UserNameGenerator(
        user_context["settings"],
        parse_obj_as(UsernameGeneratorConfig, mapping["username_generator"]),
        user_context["dataloader"],
        user_context["ldap_connection"],
    )

    result = await username_generator.get_existing_values(["sAMAccountName", "cn"])
    assert result == {"cn": ["foo"], "sAMAccountName": []}

    result = await username_generator.get_existing_values(["employeeID"])
    assert result == {"employeeID": ["0101700001"]}


async def test_get_existing_names(sync_tool: SyncTool, context: Context) -> None:
    settings = context["user_context"]["settings"]
    settings = settings.copy(update={"ldap_dialect": "Standard"})
    context["user_context"]["settings"] = settings

    mapping = {
        "mo_to_ldap": {"Employee": {}},
        "username_generator": {
            "objectClass": "UserNameGenerator",
        },
    }

    user_context = context["user_context"]
    username_generator = UserNameGenerator(
        user_context["settings"],
        parse_obj_as(UsernameGeneratorConfig, mapping["username_generator"]),
        user_context["dataloader"],
        user_context["ldap_connection"],
    )

    result = await username_generator._get_existing_names()
    assert result == ([], ["foo"])


async def test_load_ldap_OUs(
    ldap_connection: Connection,
    ldap_container_dn: str,
    context: Context,
) -> None:
    group_dn1 = f"OU=Users,{ldap_container_dn}"
    ldap_connection.strategy.add_entry(
        group_dn1,
        {
            "objectClass": "organizationalUnit",
            "ou": "Users",
            "revision": 1,
            "entryUUID": "{" + str(uuid4()) + "}",
        },
    )
    group_dn2 = f"OU=Groups,{ldap_container_dn}"
    ldap_connection.strategy.add_entry(
        group_dn2,
        {
            "objectClass": "organizationalUnit",
            "ou": "Groups",
            "revision": 1,
            "entryUUID": "{" + str(uuid4()) + "}",
        },
    )

    user_dn = f"CN=Nick Janssen,{group_dn1}"
    ldap_connection.strategy.add_entry(
        user_dn,
        {
            "objectClass": "inetOrgPerson",
            "userPassword": str(uuid4()),
            "sn": "Janssen",
            "revision": 1,
            "entryUUID": "{" + str(uuid4()) + "}",
            "employeeID": "0101700001",
        },
    )

    settings = context["user_context"]["settings"]
    output = await load_ldap_OUs(settings, ldap_connection, ldap_container_dn)

    ou1 = extract_ou_from_dn(group_dn1)
    ou2 = extract_ou_from_dn(group_dn2)
    assert output == {
        ou1: {"empty": True, "dn": group_dn1},
        ou2: {"empty": True, "dn": group_dn2},
    }
