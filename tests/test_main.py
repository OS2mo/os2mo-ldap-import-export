# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument
# pylint: disable=protected-access
import asyncio
import datetime
import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import call
from unittest.mock import create_autospec
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import UUID
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fastramqpi.main import FastRAMQPI
from fastramqpi.ramqp import AMQPSystem
from fastramqpi.ramqp.depends import Context
from fastramqpi.ramqp.depends import get_context
from fastramqpi.ramqp.utils import RejectMessage
from fastramqpi.ramqp.utils import RequeueMessage
from gql.transport.exceptions import TransportQueryError
from pydantic import parse_obj_as
from ramodels.mo.details.address import Address
from ramodels.mo.details.it_system import ITUser
from ramodels.mo.employee import Employee
from structlog.testing import capture_logs

from mo_ldap_import_export.autogenerated_graphql_client.client import GraphQLClient
from mo_ldap_import_export.config import ConversionMapping
from mo_ldap_import_export.exceptions import IgnoreChanges
from mo_ldap_import_export.exceptions import IncorrectMapping
from mo_ldap_import_export.exceptions import NoObjectsReturnedException
from mo_ldap_import_export.exceptions import NotSupportedException
from mo_ldap_import_export.exceptions import ReadOnlyException
from mo_ldap_import_export.ldap_classes import LdapObject
from mo_ldap_import_export.main import create_app
from mo_ldap_import_export.main import create_fastramqpi
from mo_ldap_import_export.main import initialize_checks
from mo_ldap_import_export.main import initialize_converters
from mo_ldap_import_export.main import initialize_info_dict_refresher
from mo_ldap_import_export.main import initialize_init_engine
from mo_ldap_import_export.main import initialize_ldap_listener
from mo_ldap_import_export.main import initialize_sync_tool
from mo_ldap_import_export.main import open_ldap_connection
from mo_ldap_import_export.main import process_address
from mo_ldap_import_export.main import process_engagement
from mo_ldap_import_export.main import process_ituser
from mo_ldap_import_export.main import process_org_unit
from mo_ldap_import_export.main import process_person
from mo_ldap_import_export.main import reject_on_failure
from mo_ldap_import_export.usernames import get_username_generator_class
from mo_ldap_import_export.usernames import UserNameGeneratorBase
from mo_ldap_import_export.utils import get_delete_flag
from tests.graphql_mocker import GraphQLMocker


@pytest.fixture(scope="session")
def event_loop():
    """Overrides pytest default function scoped event loop"""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
def settings_overrides() -> Iterator[dict[str, str]]:
    """Fixture to construct dictionary of minimal overrides for valid settings.

    Yields:
        Minimal set of overrides.
    """
    conversion_mapping_dict = {
        "ldap_to_mo": {
            "Employee": {
                "objectClass": "ramodels.mo.employee.Employee",
                "_import_to_mo_": "false",
                "uuid": "{{ employee_uuid or NONE }}",
            }
        },
        "mo_to_ldap": {
            "Employee": {
                "objectClass": "inetOrgPerson",
                "_export_to_ldap_": "false",
            }
        },
        "username_generator": {"objectClass": "UserNameGenerator"},
    }
    conversion_mapping = parse_obj_as(ConversionMapping, conversion_mapping_dict)
    conversion_mapping_setting = conversion_mapping.json(
        exclude_unset=True, by_alias=True
    )
    overrides = {
        "CONVERSION_MAPPING": conversion_mapping_setting,
        "CLIENT_ID": "Foo",
        "CLIENT_SECRET": "bar",
        "LDAP_CONTROLLERS": '[{"host": "localhost"}]',
        "LDAP_DOMAIN": "LDAP",
        "LDAP_USER": "foo",
        "LDAP_PASSWORD": "foo",
        "LDAP_SEARCH_BASE": "DC=ad,DC=addev",
        "DEFAULT_ORG_UNIT_LEVEL": "foo",
        "DEFAULT_ORG_UNIT_TYPE": "foo",
        "LDAP_OUS_TO_SEARCH_IN": '["OU=bar"]',
        "LDAP_OU_FOR_NEW_USERS": "OU=foo,OU=bar",
        "FASTRAMQPI__AMQP__URL": "amqp://guest:guest@msg_broker:5672/",
    }
    yield overrides


@pytest.fixture(scope="module")
def load_settings_overrides(
    settings_overrides: dict[str, str],
) -> Iterator[dict[str, str]]:
    """Fixture to set happy-path settings overrides as environmental variables.

    Note:
        Only loads environmental variables, if variables are not already set.

    Args:
        settings_overrides: The list of settings to load in.
        monkeypatch: Pytest MonkeyPatch instance to set environmental variables.

    Yields:
        Minimal set of overrides.
    """

    mp = pytest.MonkeyPatch()
    for key, value in settings_overrides.items():
        if os.environ.get(key) is None:
            mp.setenv(key, value)
    yield settings_overrides


@pytest.fixture(scope="module")
def test_mo_address() -> Address:
    test_mo_address = Address.from_simplified_fields(
        "foo@bar.dk", uuid4(), "2021-01-01"
    )
    return test_mo_address


@pytest.fixture(scope="module")
def test_mo_objects() -> list:
    return [
        {
            "uuid": uuid4(),
            "service_type": "employee",
            "payload": uuid4(),
            "parent_uuid": uuid4(),
            "object_type": "person",
            "validity": {
                "from": datetime.datetime.today().strftime("%Y-%m-%d"),
                "to": None,
            },
        },
        {
            "uuid": uuid4(),
            "service_type": "employee",
            "payload": uuid4(),
            "parent_uuid": uuid4(),
            "object_type": "person",
            "validity": {
                "from": "2021-01-01",
                "to": datetime.datetime.today().strftime("%Y-%m-%d"),
            },
        },
        {
            "uuid": uuid4(),
            "service_type": "employee",
            "payload": uuid4(),
            "parent_uuid": uuid4(),
            "object_type": "person",
            "validity": {
                "from": "2021-01-01",
                "to": "2021-05-01",
            },
        },
        {
            "uuid": uuid4(),
            "service_type": "employee",
            "payload": uuid4(),
            "parent_uuid": uuid4(),
            "object_type": "person",
            "validity": {
                "from": datetime.datetime.today().strftime("%Y-%m-%d"),
                "to": datetime.datetime.today().strftime("%Y-%m-%d"),
            },
        },
    ]


@pytest.fixture(scope="module")
def dataloader(
    sync_dataloader: MagicMock, test_mo_address: Address, test_mo_objects: list
) -> Iterator[AsyncMock]:
    objectGUID1 = UUID("00000000-0000-4000-1000-000000000000")
    objectGUID2 = UUID("00000000-0000-4000-2000-000000000000")
    objectGUID3 = UUID("00000000-0000-4000-3000-000000000000")
    test_ldap_object1 = LdapObject(
        name="Tester1",
        Department="QA",
        dn="someDN1",
        EmployeeID="0101012001",
        objectGUID=objectGUID1,
    )
    test_ldap_object2 = LdapObject(
        name="Tester2",
        Department="QA",
        dn="someDN2",
        EmployeeID="0101012002",
        objectGUID=objectGUID2,
    )
    test_ldap_object3 = LdapObject(
        name="Tester3",
        Department="QA",
        dn="someDN3",
        EmployeeID="0101012003",
        objectGUID=objectGUID3,
    )
    test_mo_employee = Employee(cpr_no="1212121234")

    test_mo_it_user = ITUser.from_simplified_fields("foo", uuid4(), "2021-01-01")

    load_ldap_cpr_object = AsyncMock()
    load_ldap_cpr_object.return_value = test_ldap_object1

    dataloader = AsyncMock()
    dataloader.get_ldap_dn = AsyncMock()
    dataloader.load_ldap_object = AsyncMock()
    dataloader.load_ldap_populated_overview = sync_dataloader
    dataloader.load_ldap_OUs = AsyncMock()
    dataloader.load_ldap_overview = sync_dataloader
    dataloader.load_ldap_cpr_object = load_ldap_cpr_object
    dataloader.load_mo_employee.return_value = test_mo_employee
    dataloader.load_mo_address.return_value = test_mo_address
    dataloader.load_mo_it_user.return_value = test_mo_it_user
    dataloader.load_mo_employee_address_types.return_value = None
    dataloader.load_mo_org_unit_address_types.return_value = None
    dataloader.load_mo_it_systems.return_value = None
    dataloader.load_mo_primary_types.return_value = None
    dataloader.load_mo_employee_addresses.return_value = [test_mo_address] * 2
    dataloader.load_all_mo_objects.return_value = test_mo_objects
    dataloader.load_mo_object.return_value = test_mo_objects[0]
    dataloader.load_ldap_attribute_values = sync_dataloader
    dataloader.modify_ldap_object.return_value = [{"description": "success"}]
    dataloader.get_ldap_unique_ldap_uuid = AsyncMock()
    dataloader.get_ldap_it_system_uuid = sync_dataloader
    dataloader.supported_object_types = ["address", "person"]
    with patch(
        "mo_ldap_import_export.routes.load_ldap_objects",
        return_value=[
            test_ldap_object1,
            test_ldap_object2,
            test_ldap_object3,
        ],
    ):
        yield dataloader


@pytest.fixture(scope="module")
def sync_dataloader() -> MagicMock:
    dataloader = MagicMock()
    return dataloader


@pytest.fixture(scope="module")
def converter() -> MagicMock:
    converter = MagicMock()
    converter.get_mo_to_ldap_json_keys.return_value = [
        "Employee",
        "Address",
        "EmailEmployee",
    ]
    converter.cpr_field = "EmployeeID"
    converter.ldap_it_system = "ADGUID"
    converter._import_to_mo_ = MagicMock()
    converter._import_to_mo_.return_value = True

    converter.to_ldap = AsyncMock()
    converter.from_ldap = AsyncMock()
    converter.from_ldap.return_value = Employee(name="Angus")

    converter.load_info_dicts = AsyncMock()
    converter._init = AsyncMock()

    return converter


@pytest.fixture(scope="module")
def sync_tool() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def context_dependency_injection(
    app: FastAPI, fastramqpi: FastRAMQPI
) -> Iterator[Context]:
    context = fastramqpi.get_context()

    def context_extractor() -> Any:
        return context

    app.dependency_overrides[get_context] = context_extractor

    yield context

    del app.dependency_overrides[get_context]


@pytest.fixture(scope="module")
def patch_modules(
    load_settings_overrides: dict[str, str],
    dataloader: AsyncMock,
) -> Iterator[None]:
    """
    Fixture to patch modules needed in main.py
    """
    with patch(
        "mo_ldap_import_export.main.configure_ldap_connection", new_callable=MagicMock()
    ), patch("mo_ldap_import_export.main.DataLoader", return_value=dataloader), patch(
        "mo_ldap_import_export.routes.get_attribute_types", return_value={"foo": {}}
    ):
        yield


@pytest.fixture(scope="module")
def fastramqpi(patch_modules: None) -> FastRAMQPI:
    return create_fastramqpi()


@pytest.fixture(scope="module")
def app(fastramqpi: FastRAMQPI) -> Iterator[FastAPI]:
    """Fixture to construct a FastAPI application.

    Yields:
        FastAPI application.
    """
    with patch("mo_ldap_import_export.main.create_fastramqpi", return_value=fastramqpi):
        yield create_app()


@pytest.fixture(scope="module")
def test_client(app: FastAPI) -> Iterator[TestClient]:
    """Fixture to construct a FastAPI test-client.

    Note:
        The app does not do lifecycle management.

    Yields:
        TestClient for the FastAPI application.
    """
    yield TestClient(app)


@pytest.fixture(autouse=True, scope="module")
def always_create_fastramqpi(patch_modules: None) -> None:
    """Test that we can construct our FastRAMQPI system."""
    fastramqpi = create_fastramqpi()
    assert isinstance(fastramqpi, FastRAMQPI)


def test_create_app() -> None:
    """Test that we can construct our FastAPI application."""
    app = create_app()
    assert isinstance(app, FastAPI)


@pytest.fixture(autouse=True, scope="module")
async def always_initialize_sync_tool(
    fastramqpi: FastRAMQPI, sync_tool: AsyncMock
) -> None:
    user_context = fastramqpi.get_context()["user_context"]
    assert user_context.get("sync_tool") is None

    with patch("mo_ldap_import_export.main.SyncTool", return_value=sync_tool):
        async with initialize_sync_tool(fastramqpi):
            assert user_context.get("sync_tool") is not None


@pytest.fixture(autouse=True, scope="module")
async def always_initialize_ldap_listener(fastramqpi: FastRAMQPI) -> None:
    user_context = fastramqpi.get_context()["user_context"]
    assert user_context.get("pollers") is None

    async with initialize_ldap_listener(fastramqpi):
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert user_context.get("pollers") is not None


@pytest.fixture(autouse=True, scope="module")
async def always_initialize_checks(fastramqpi: FastRAMQPI) -> None:
    user_context = fastramqpi.get_context()["user_context"]
    assert user_context.get("export_checks") is None
    assert user_context.get("import_checks") is None

    with patch("mo_ldap_import_export.main.ExportChecks", return_value=MagicMock()):
        with patch("mo_ldap_import_export.main.ImportChecks", return_value=MagicMock()):
            async with initialize_checks(fastramqpi):
                assert user_context.get("export_checks") is not None
                assert user_context.get("import_checks") is not None


@pytest.fixture(autouse=True, scope="module")
async def always_initialize_converter(
    fastramqpi: FastRAMQPI, converter: MagicMock
) -> None:
    user_context = fastramqpi.get_context()["user_context"]
    assert user_context.get("converter") is None
    assert user_context.get("ldap_it_system_user_key") is None
    assert user_context.get("cpr_field") is None

    with patch("mo_ldap_import_export.main.LdapConverter", return_value=converter):
        async with initialize_converters(fastramqpi):
            assert user_context.get("converter") is not None
            assert user_context.get("ldap_it_system_user_key") == "ADGUID"
            assert user_context.get("cpr_field") == "EmployeeID"


@pytest.fixture(autouse=True, scope="module")
async def always_initialize_init_engine(fastramqpi: FastRAMQPI) -> None:
    user_context = fastramqpi.get_context()["user_context"]
    assert user_context.get("init_engine") is None

    init_engine = AsyncMock()

    with patch("mo_ldap_import_export.main.InitEngine", return_value=init_engine):
        async with initialize_init_engine(fastramqpi):
            assert user_context.get("init_engine") is not None
            init_engine.create_facets.assert_called_once()
            init_engine.create_it_systems.assert_called_once()


async def test_open_ldap_connection() -> None:
    """Test the open_ldap_connection."""
    state = []

    @contextmanager
    def manager() -> Iterator[None]:
        state.append(1)
        yield
        state.append(2)

    ldap_connection = manager()

    assert not state
    async with open_ldap_connection(ldap_connection):  # type: ignore
        assert state == [1]
    assert state == [1, 2]


@pytest.mark.usefixtures("context_dependency_injection")
def test_ldap_get_all_endpoint(test_client: TestClient) -> None:
    """Test the LDAP get-all endpoint on our app."""

    response = test_client.get("/LDAP/Employee", params={"entries_to_return": 20})
    assert response.status_code == 202


@pytest.mark.usefixtures("context_dependency_injection")
def test_ldap_get_all_converted_endpoint(test_client: TestClient) -> None:
    """Test the LDAP get-all endpoint on our app."""

    response = test_client.get("/LDAP/Employee/converted")
    assert response.status_code == 202


@pytest.mark.usefixtures("context_dependency_injection")
def test_ldap_get_converted_endpoint(test_client: TestClient) -> None:
    """Test the LDAP get endpoint on our app."""

    response = test_client.get("/LDAP/Employee/010101-1234/converted")
    assert response.status_code == 202

    response = test_client.get("/LDAP/Employee/invalid_cpr/converted")
    assert response.status_code == 422


@pytest.mark.usefixtures("context_dependency_injection")
def test_ldap_post_ldap_employee_endpoint(test_client: TestClient) -> None:
    """Test the LDAP get-all endpoint on our app."""

    ldap_person_to_post = {
        "dn": "CN=Lars Peter Thomsen,OU=Users,OU=Magenta,DC=ad,DC=addev",
        "cpr": "0101121234",
        "givenname": "Lars Peter",
        "surname": "Thomsen",
        "Department": None,
    }
    response = test_client.post("/LDAP/Employee", json=ldap_person_to_post)
    assert response.status_code == 200


@pytest.mark.usefixtures("context_dependency_injection")
def test_ldap_get_organizationalUser_endpoint(test_client: TestClient) -> None:
    """Test the LDAP get endpoint on our app."""

    response = test_client.get("/LDAP/Employee/010101-1234")
    assert response.status_code == 202

    response = test_client.get("/LDAP/Employee/invalid_cpr")
    assert response.status_code == 422


@pytest.mark.usefixtures("context_dependency_injection")
def test_ldap_get_overview_endpoint(test_client: TestClient) -> None:
    """Test the LDAP get endpoint on our app."""

    response = test_client.get("/Inspect/overview")
    assert response.status_code == 202


@pytest.mark.usefixtures("context_dependency_injection")
def test_ldap_get_structure_endpoint(
    test_client: TestClient, dataloader: AsyncMock
) -> None:
    """Test the LDAP get endpoint on our app."""

    dataloader.load_ldap_OUs.return_value = []

    response = test_client.get("/Inspect/structure")
    assert response.status_code == 202


@pytest.mark.usefixtures("context_dependency_injection")
def test_ldap_get_populated_overview_endpoint(test_client: TestClient) -> None:
    """Test the LDAP get endpoint on our app."""

    response = test_client.get("/Inspect/overview/populated")
    assert response.status_code == 202


@pytest.mark.usefixtures("context_dependency_injection")
def test_load_unique_attribute_values_from_LDAP_endpoint(
    test_client: TestClient,
) -> None:
    """Test the LDAP get endpoint on our app."""

    response = test_client.get("/Inspect/attribute/values/foo")
    assert response.status_code == 202


@pytest.mark.usefixtures("context_dependency_injection")
def test_ldap_get_attribute_details_endpoint(test_client: TestClient) -> None:
    """Test the LDAP get endpoint on our app."""

    response = test_client.get("/Inspect/attribute/foo")
    assert response.status_code == 202


@pytest.mark.usefixtures("context_dependency_injection")
def test_ldap_get_object_by_objectGUID_endpoint(
    test_client: TestClient, dataloader: AsyncMock
) -> None:
    """Test the LDAP get endpoint on our app."""

    dataloader.load_ldap_object.return_value = LdapObject(dn="CN=foo")

    uuid = uuid4()
    params = {"unique_ldap_uuid": str(uuid)}
    response = test_client.get("/Inspect/object/unique_ldap_uuid", params=params)
    assert response.status_code == 202


@pytest.mark.usefixtures("context_dependency_injection")
def test_ldap_get_object_by_dn_endpoint(
    test_client: TestClient, dataloader: AsyncMock
) -> None:
    """Test the LDAP get endpoint on our app."""

    dataloader.load_ldap_object.return_value = LdapObject(dn="CN=foo")

    params = {"dn": "CN=foo"}
    response = test_client.get("/Inspect/object/dn", params=params)
    assert response.status_code == 202


async def test_listen_to_ituser(graphql_mock: GraphQLMocker) -> None:
    amqpsystem = create_autospec(AMQPSystem)
    amqpsystem.exchange_name = "wow"

    graphql_client = GraphQLClient("http://example.com/graphql")

    employee_uuid = uuid4()

    employee_route = graphql_mock.query("read_ituser_employee_uuid")
    employee_route.result = {
        "itusers": {"objects": [{"current": {"employee_uuid": employee_uuid}}]}
    }

    employee_refresh_route = graphql_mock.query("employee_refresh")
    employee_refresh_route.result = {"employee_refresh": {"objects": [employee_uuid]}}

    await process_ituser(employee_uuid, graphql_client, amqpsystem)
    assert employee_refresh_route.called


@pytest.mark.parametrize(
    "objects,error",
    [
        # Must return exactly one result
        ([], "Unable to lookup ITUser"),
        ([{"current": None}, {"current": None}], "Unable to lookup ITUser"),
        # Must have current result
        ([{"current": None}], "ITUser not currently active"),
        # Must have an UUID set in employee_uuid
        ([{"current": {"employee_uuid": None}}], "ITUser not attached to a person"),
    ],
)
async def test_listen_to_ituser_failure(
    graphql_mock: GraphQLMocker,
    objects: list[dict[str, Any]],
    error: str,
) -> None:
    amqpsystem = create_autospec(AMQPSystem)
    amqpsystem.exchange_name = "wow"

    graphql_client = GraphQLClient("http://example.com/graphql")

    employee_route = graphql_mock.query("read_ituser_employee_uuid")
    employee_route.result = {"itusers": {"objects": objects}}

    employee_uuid = uuid4()
    with pytest.raises(RejectMessage) as exc_info:
        await process_ituser(employee_uuid, graphql_client, amqpsystem)
    assert error in str(exc_info.value)


async def test_listen_to_engagement(graphql_mock: GraphQLMocker) -> None:
    amqpsystem = create_autospec(AMQPSystem)
    amqpsystem.exchange_name = "wow"

    graphql_client = GraphQLClient("http://example.com/graphql")

    employee_uuid = uuid4()

    employee_route = graphql_mock.query("read_engagement_employee_uuid")
    employee_route.result = {
        "engagements": {"objects": [{"current": {"employee_uuid": employee_uuid}}]}
    }

    employee_refresh_route = graphql_mock.query("employee_refresh")
    employee_refresh_route.result = {"employee_refresh": {"objects": [employee_uuid]}}

    await process_engagement(employee_uuid, graphql_client, amqpsystem)
    assert employee_refresh_route.called


@pytest.mark.parametrize(
    "objects,error",
    [
        # Must return exactly one result
        ([], "Unable to lookup engagement"),
        ([{"current": None}, {"current": None}], "Unable to lookup engagement"),
        # Must have current result
        ([{"current": None}], "Engagement not currently active"),
    ],
)
async def test_listen_to_engagement_failure(
    graphql_mock: GraphQLMocker,
    objects: list[dict[str, Any]],
    error: str,
) -> None:
    amqpsystem = create_autospec(AMQPSystem)
    amqpsystem.exchange_name = "wow"

    graphql_client = GraphQLClient("http://example.com/graphql")

    employee_route = graphql_mock.query("read_engagement_employee_uuid")
    employee_route.result = {"engagements": {"objects": objects}}

    employee_uuid = uuid4()
    with pytest.raises(RejectMessage) as exc_info:
        await process_engagement(employee_uuid, graphql_client, amqpsystem)
    assert error in str(exc_info.value)


async def test_listen_to_address_person(graphql_mock: GraphQLMocker) -> None:
    amqpsystem = create_autospec(AMQPSystem)
    amqpsystem.exchange_name = "wow"

    graphql_client = GraphQLClient("http://example.com/graphql")

    address_uuid = uuid4()
    employee_uuid = uuid4()

    employee_route = graphql_mock.query("read_address_relation_uuids")
    employee_route.result = {
        "addresses": {
            "objects": [
                {"current": {"employee_uuid": employee_uuid, "org_unit_uuid": None}}
            ]
        }
    }

    employee_refresh_route = graphql_mock.query("employee_refresh")
    employee_refresh_route.result = {"employee_refresh": {"objects": [employee_uuid]}}

    await process_address(address_uuid, graphql_client, amqpsystem)
    assert employee_refresh_route.called


async def test_listen_to_address_org_unit(graphql_mock: GraphQLMocker) -> None:
    amqpsystem = create_autospec(AMQPSystem)
    amqpsystem.exchange_name = "wow"

    graphql_client = GraphQLClient("http://example.com/graphql")

    org_unit_uuid = uuid4()
    employee_uuid = uuid4()
    address_uuid = uuid4()

    address_route = graphql_mock.query("read_address_relation_uuids")
    address_route.result = {
        "addresses": {
            "objects": [
                {"current": {"employee_uuid": None, "org_unit_uuid": org_unit_uuid}}
            ]
        }
    }

    employee_route = graphql_mock.query("read_employees_with_engagement_to_org_unit")
    employee_route.result = {
        "engagements": {"objects": [{"current": {"employee_uuid": employee_uuid}}]}
    }

    employee_refresh_route = graphql_mock.query("employee_refresh")
    employee_refresh_route.result = {"employee_refresh": {"objects": [employee_uuid]}}

    await process_address(address_uuid, graphql_client, amqpsystem)
    assert employee_refresh_route.called


@pytest.mark.parametrize(
    "objects,error",
    [
        # Must return exactly one result
        ([], "Unable to lookup address"),
        ([{"current": None}, {"current": None}], "Unable to lookup address"),
        # Must have current result
        ([{"current": None}], "Address not currently active"),
    ],
)
async def test_listen_to_address_failure(
    graphql_mock: GraphQLMocker,
    objects: list[dict[str, Any]],
    error: str,
) -> None:
    amqpsystem = create_autospec(AMQPSystem)
    amqpsystem.exchange_name = "wow"

    graphql_client = GraphQLClient("http://example.com/graphql")

    employee_route = graphql_mock.query("read_address_relation_uuids")
    employee_route.result = {"addresses": {"objects": objects}}

    address_uuid = uuid4()
    with pytest.raises(RejectMessage) as exc_info:
        await process_address(address_uuid, graphql_client, amqpsystem)
    assert error in str(exc_info.value)


async def test_listen_to_changes(sync_tool: AsyncMock) -> None:
    settings = MagicMock()
    settings.listen_to_changes_in_mo = True

    payload = uuid4()

    sync_tool.reset_mock()
    await process_person(payload, sync_tool)
    sync_tool.listen_to_changes_in_employees.assert_awaited_once()

    sync_tool.reset_mock()
    await process_org_unit(payload, sync_tool)
    sync_tool.publish_engagements_for_org_unit.assert_awaited_once_with(payload)


@pytest.mark.usefixtures("context_dependency_injection")
def test_ldap_get_all_converted_endpoint_failure(
    test_client: TestClient,
    converter: MagicMock,
) -> None:
    async def from_ldap(ldap_object, json_key, employee_uuid=None):
        # This will raise a validationError (which is what we want to test)
        return Employee(**{"foo": None})

    converter.from_ldap = from_ldap
    response1 = test_client.get("/LDAP/Employee/converted")
    response2 = test_client.get("/LDAP/Employee/010101-1234/converted")

    assert response1.status_code == 202
    assert response2.status_code == 404


async def test_import_all_objects_from_LDAP_first_20(
    context_dependency_injection: Context, test_client: TestClient
) -> None:
    ldap_amqpsystem = create_autospec(AMQPSystem)

    context = context_dependency_injection
    context["user_context"]["ldap_amqpsystem"] = ldap_amqpsystem

    params = {
        "test_on_first_20_entries": True,
    }
    response = test_client.get("/Import", params=params)
    assert response.status_code == 202

    assert ldap_amqpsystem.publish_message.mock_calls == [
        call("uuid", UUID("00000000-0000-4000-1000-000000000000")),
        call("uuid", UUID("00000000-0000-4000-2000-000000000000")),
        call("uuid", UUID("00000000-0000-4000-3000-000000000000")),
    ]


async def test_import_all_objects_from_LDAP(
    context_dependency_injection: Context, test_client: TestClient
) -> None:
    ldap_amqpsystem = create_autospec(AMQPSystem)

    context = context_dependency_injection
    context["user_context"]["ldap_amqpsystem"] = ldap_amqpsystem

    response = test_client.get("/Import")
    assert response.status_code == 202

    assert ldap_amqpsystem.publish_message.mock_calls == [
        call("uuid", UUID("00000000-0000-4000-1000-000000000000")),
        call("uuid", UUID("00000000-0000-4000-2000-000000000000")),
        call("uuid", UUID("00000000-0000-4000-3000-000000000000")),
    ]


@pytest.mark.usefixtures("context_dependency_injection")
async def test_import_one_object_from_LDAP(test_client: TestClient) -> None:
    uuid = uuid4()
    response = test_client.get(f"/Import/{uuid}")
    assert response.status_code == 202


@pytest.mark.usefixtures("context_dependency_injection")
async def test_import_all_objects_from_LDAP_no_cpr_field(
    test_client: TestClient, converter: MagicMock
) -> None:
    converter.cpr_field = None
    response = test_client.get("/Import?cpr_indexed_entries_only=True")
    assert response.status_code == 404
    converter.cpr_field = "EmployeeID"


@pytest.mark.usefixtures("context_dependency_injection")
async def test_import_all_objects_from_LDAP_invalid_cpr(
    test_client: TestClient, dataloader: AsyncMock
) -> None:
    with patch(
        "mo_ldap_import_export.routes.load_ldap_objects",
        return_value=[
            LdapObject(
                name="Tester", Department="QA", dn="someDN", EmployeeID="5001012002"
            )
        ],
    ), capture_logs() as cap_logs:
        response = test_client.get("/Import")
        assert response.status_code == 202

        messages = [w for w in cap_logs if w["log_level"] == "info"]
        assert "Invalid CPR Number found" in str(messages)


async def test_incorrect_ous_to_search_in() -> None:
    mp = pytest.MonkeyPatch()
    overrides = {
        "LDAP_OUS_TO_SEARCH_IN": '["OU=bar"]',
        "LDAP_OU_FOR_NEW_USERS": "OU=foo,",
    }
    for key, value in overrides.items():
        mp.setenv(key, value)

    with pytest.raises(ValueError):
        create_fastramqpi()

    overrides = {
        "LDAP_OUS_TO_SEARCH_IN": '["OU=bar"]',
        "LDAP_OU_FOR_NEW_USERS": "OU=foo,OU=bar",
    }
    for key, value in overrides.items():
        mp.setenv(key, value)


async def test_load_faulty_username_generator() -> None:
    username_generators = ["UserNameGenerator", "AlleroedUserNameGenerator"]
    for username_generator in username_generators:
        clazz = get_username_generator_class(username_generator)
        assert issubclass(clazz, UserNameGeneratorBase)

    with pytest.raises(ValueError) as exc_info:
        get_username_generator_class("__unknown_username_generator")
    assert "No such username_generator" in str(exc_info.value)


async def test_reject_on_failure():
    async def not_supported_func():
        raise NotSupportedException("")

    async def incorrect_mapping_func():
        raise IncorrectMapping("")

    async def transport_query_error_func():
        raise TransportQueryError("")

    async def no_objects_returned_func():
        raise NoObjectsReturnedException("")

    async def requeue_error_func():
        raise RequeueMessage("")

    async def ignore_changes_func():
        raise IgnoreChanges("")

    async def read_only_func():
        raise ReadOnlyException("")

    # These exceptions should result in RequeueMessage exceptions
    for func in [
        not_supported_func,
        incorrect_mapping_func,
        transport_query_error_func,
        no_objects_returned_func,
    ]:
        with pytest.raises(RequeueMessage):
            await reject_on_failure(func)()

    with pytest.raises(RequeueMessage):
        await reject_on_failure(requeue_error_func)()

    for func in [ignore_changes_func, read_only_func]:
        with pytest.raises(RejectMessage):
            await reject_on_failure(func)()


async def test_get_delete_flag(dataloader: AsyncMock):
    # When there are matching objects in MO, but the to-date is today, delete
    mo_object = {"validity": {"to": datetime.datetime.today().strftime("%Y-%m-%d")}}

    flag = get_delete_flag(mo_object)
    assert flag is True

    # When there are matching objects in MO, but the to-date is tomorrow, do not delete
    mo_object = {
        "validity": {
            "to": (datetime.datetime.today() + datetime.timedelta(1)).strftime(
                "%Y-%m-%d"
            )
        }
    }

    flag = get_delete_flag(mo_object)
    assert flag is False


@pytest.mark.usefixtures("context_dependency_injection")
def test_get_invalid_cpr_numbers_from_LDAP_endpoint(
    test_client: TestClient,
    dataloader: AsyncMock,
):
    valid_object = LdapObject(dn="foo", EmployeeID="0101011234")
    invalid_object = LdapObject(dn="bar", EmployeeID="ja")

    with patch(
        "mo_ldap_import_export.routes.load_ldap_objects",
        return_value=[valid_object, invalid_object],
    ):
        response = test_client.get("/Inspect/invalid_cpr_numbers")
    assert response.status_code == 202
    result = response.json()
    assert "bar" in result
    assert result["bar"] == "ja"


@pytest.mark.usefixtures("context_dependency_injection")
def test_get_invalid_cpr_numbers_from_LDAP_endpoint_no_cpr_field(
    test_client: TestClient, converter: MagicMock
):
    converter.cpr_field = None
    response = test_client.get("/Inspect/invalid_cpr_numbers")
    assert response.status_code == 404
    converter.cpr_field = "EmployeeID"


def test_wraps():
    """
    Test that the decorated listen_to_changes function keeps its name
    """
    assert process_address.__name__ == "process_address"
    assert process_engagement.__name__ == "process_engagement"
    assert process_ituser.__name__ == "process_ituser"
    assert process_person.__name__ == "process_person"
    assert process_org_unit.__name__ == "process_org_unit"


@pytest.mark.usefixtures("context_dependency_injection")
def test_get_duplicate_cpr_numbers_from_LDAP_endpoint_no_cpr_field(
    test_client: TestClient, converter: MagicMock
):
    converter.cpr_field = None
    response = test_client.get("/Inspect/duplicate_cpr_numbers")
    assert response.status_code == 404
    converter.cpr_field = "EmployeeID"


@pytest.mark.usefixtures("context_dependency_injection")
def test_get_duplicate_cpr_numbers_from_LDAP_endpoint(
    test_client: TestClient,
):
    searchResponse = [
        {"dn": "foo", "attributes": {"EmployeeID": "12"}},
        {"dn": "mucki", "attributes": {"EmployeeID": "123"}},
        {"dn": "bar", "attributes": {"EmployeeID": "123"}},
    ]

    with patch(
        "mo_ldap_import_export.routes.paged_search", return_value=searchResponse
    ):
        response = test_client.get("/Inspect/duplicate_cpr_numbers")
        assert response.status_code == 202
        result = response.json()
        assert "123" in result
        assert "mucki" in result["123"]
        assert "bar" in result["123"]


@pytest.mark.usefixtures("context_dependency_injection")
async def test_get_non_existing_objectGUIDs_from_MO(
    dataloader: AsyncMock,
    test_client: TestClient,
) -> None:
    dataloader.get_ldap_it_system_uuid.return_value = str(uuid4())

    it_users = [
        {"employee_uuid": str(uuid4()), "user_key": str(uuid4())},
        {"employee_uuid": str(uuid4()), "user_key": str(uuid4())},
        {"employee_uuid": str(uuid4()), "user_key": "foo"},
    ]
    employee = Employee(givenname="Jim", surname="")
    dataloader.load_mo_employee.return_value = employee

    with patch(
        "mo_ldap_import_export.routes.load_ldap_attribute_values",
        return_value=[
            it_users[0]["user_key"],
            str(uuid4()),
        ],
    ), patch(
        "mo_ldap_import_export.routes.load_all_current_it_users", return_value=it_users
    ):
        response = test_client.get("/Inspect/non_existing_unique_ldap_uuids")
    assert response.status_code == 202

    result = response.json()
    assert len(result) == 2
    assert result[0]["MO employee uuid"] == str(employee.uuid)
    assert result[0]["name"] == "Jim"
    assert result[0]["unique_ldap_uuid in MO"] == it_users[1]["user_key"]
    assert result[1]["unique_ldap_uuid in MO"] == it_users[2]["user_key"]


@pytest.mark.usefixtures("context_dependency_injection")
async def test_get_non_existing_objectGUIDs_from_MO_404(
    dataloader: AsyncMock,
    test_client: TestClient,
) -> None:
    dataloader.get_ldap_it_system_uuid.return_value = None
    response = test_client.get("/Inspect/non_existing_unique_ldap_uuids")
    assert response.status_code == 404


async def test_initialize_info_dict_refresher_once() -> None:
    converter = AsyncMock()

    fastramqpi = MagicMock()
    fastramqpi._context = {"user_context": {"converter": converter}}

    async with initialize_info_dict_refresher(fastramqpi):
        await asyncio.sleep(0)

    converter.load_info_dicts.assert_awaited_once()


async def test_initialize_info_dict_refresher_multiple_loops() -> None:
    converter = AsyncMock()

    fastramqpi = MagicMock()
    fastramqpi._context = {"user_context": {"converter": converter}}

    # Save original sleep to avoid infinite recursion in zero_sleeper
    original_sleep = asyncio.sleep

    async def zero_sleeper(_) -> None:
        await original_sleep(0)

    # Mock asyncio, so we can override sleep inside the context manager
    my_asyncio = asyncio
    my_asyncio.sleep = zero_sleeper  # type: ignore

    with patch("mo_ldap_import_export.main.asyncio", new=my_asyncio):
        async with initialize_info_dict_refresher(fastramqpi):
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            await asyncio.sleep(0)

    assert converter.load_info_dicts.await_count == 3
