# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument
# pylint: disable=protected-access
import asyncio
import datetime
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.testclient import TestClient
from fastramqpi.context import Context
from fastramqpi.main import FastRAMQPI
from gql.transport.exceptions import TransportQueryError
from ramodels.mo.employee import Employee
from ramqp.mo.models import MORoutingKey
from ramqp.mo.models import ObjectType
from ramqp.mo.models import PayloadType
from ramqp.mo.models import RequestType
from ramqp.mo.models import ServiceType
from ramqp.utils import RejectMessage
from structlog.testing import capture_logs

from mo_ldap_import_export.exceptions import IncorrectMapping
from mo_ldap_import_export.exceptions import NoObjectsReturnedException
from mo_ldap_import_export.exceptions import NotSupportedException
from mo_ldap_import_export.ldap_classes import LdapObject
from mo_ldap_import_export.main import create_app
from mo_ldap_import_export.main import create_fastramqpi
from mo_ldap_import_export.main import get_delete_flag
from mo_ldap_import_export.main import listen_to_changes
from mo_ldap_import_export.main import open_ldap_connection
from mo_ldap_import_export.main import reject_on_failure


@pytest.fixture
def load_settings_overrides_incorrect_mapping(
    settings_overrides: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> Iterator[dict[str, str]]:
    """Fixture to construct dictionary of minimal overrides for valid settings,
       but pointing to a nonexistent mapping file

    Yields:
        Minimal set of overrides.
    """
    overrides = {**settings_overrides, "CONVERSION_MAP": "nonexisting_file"}
    for key, value in overrides.items():
        if os.environ.get(key) is None:
            monkeypatch.setenv(key, value)
    yield overrides


@pytest.fixture
def load_settings_overrides_not_listening(
    settings_overrides: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> Iterator[dict[str, str]]:
    """Fixture to construct dictionary of minimal overrides for valid settings,
       but with listen_to_changes equal to False

    Yields:
        Minimal set of overrides.
    """
    overrides = {**settings_overrides, "LISTEN_TO_CHANGES_IN_MO": "False"}
    for key, value in overrides.items():
        if os.environ.get(key) is None:
            monkeypatch.setenv(key, value)
    yield overrides


@pytest.fixture
def disable_metrics(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Fixture to set the ENABLE_METRICS environmental variable to False.

    Yields:
        None
    """
    monkeypatch.setenv("ENABLE_METRICS", "False")
    yield


@pytest.fixture
def gql_client() -> Iterator[AsyncMock]:
    yield AsyncMock()


@pytest.fixture
def internal_amqpsystem() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def sync_tool() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def fastramqpi(
    disable_metrics: None,
    load_settings_overrides: dict[str, str],
    gql_client: AsyncMock,
    dataloader: AsyncMock,
    converter: MagicMock,
    internal_amqpsystem: AsyncMock,
    sync_tool: AsyncMock,
) -> Iterator[FastRAMQPI]:
    """Fixture to construct a FastRAMQPI system.

    Yields:
        FastRAMQPI system.
    """
    with patch(
        "mo_ldap_import_export.main.configure_ldap_connection", new_callable=MagicMock()
    ), patch(
        "mo_ldap_import_export.main.construct_gql_client",
        return_value=gql_client,
    ), patch(
        "mo_ldap_import_export.main.DataLoader", return_value=dataloader
    ), patch(
        "mo_ldap_import_export.main.SyncTool", return_value=sync_tool
    ), patch(
        "mo_ldap_import_export.main.LdapConverter", return_value=converter
    ), patch(
        "mo_ldap_import_export.main.get_attribute_types", return_value={"foo": {}}
    ), patch(
        "mo_ldap_import_export.main.AMQPSystem", return_value=internal_amqpsystem
    ):
        yield create_fastramqpi()


@pytest.fixture
def app(fastramqpi: FastRAMQPI) -> Iterator[FastAPI]:
    """Fixture to construct a FastAPI application.

    Yields:
        FastAPI application.
    """
    yield create_app()


@pytest.fixture
def test_client(app: FastAPI) -> Iterator[TestClient]:
    """Fixture to construct a FastAPI test-client.

    Note:
        The app does not do lifecycle management.

    Yields:
        TestClient for the FastAPI application.
    """
    yield TestClient(app)


@pytest.fixture
def test_client_no_cpr(app: FastAPI, converter: MagicMock) -> Iterator[TestClient]:
    """Fixture to construct a FastAPI test-client. where cpr_field = None

    Note:
        The app does not do lifecycle management.

    Yields:
        TestClient for the FastAPI application.
    """
    converter.cpr_field = None
    yield TestClient(create_app())


@pytest.fixture
def ldap_connection() -> Iterator[MagicMock]:
    """Fixture to construct a mock ldap_connection.

    Yields:
        A mock for ldap_connection.
    """
    yield MagicMock()


@pytest.fixture
def headers(test_client: TestClient) -> dict:
    response = test_client.post(
        "/login", data={"username": "admin", "password": "admin"}
    )
    headers = {"Authorization": "Bearer " + response.json()["access_token"]}
    return headers


def test_create_app(
    fastramqpi: FastRAMQPI,
    load_settings_overrides: dict[str, str],
) -> None:
    """Test that we can construct our FastAPI application."""

    with patch("mo_ldap_import_export.main.create_fastramqpi", return_value=fastramqpi):
        app = create_app()
    assert isinstance(app, FastAPI)


def test_create_fastramqpi(
    load_settings_overrides: dict[str, str], disable_metrics: None, converter: MagicMock
) -> None:
    """Test that we can construct our FastRAMQPI system."""

    with patch(
        "mo_ldap_import_export.main.configure_ldap_connection", new_callable=MagicMock()
    ), patch("mo_ldap_import_export.main.LdapConverter", return_value=converter):
        fastramqpi = create_fastramqpi()
    assert isinstance(fastramqpi, FastRAMQPI)


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
    async with open_ldap_connection(ldap_connection):
        assert state == [1]
    assert state == [1, 2]


def test_ldap_get_all_endpoint(test_client: TestClient, headers: dict) -> None:
    """Test the LDAP get-all endpoint on our app."""

    response = test_client.get(
        "/LDAP/Employee", headers=headers, params={"entries_to_return": 20}
    )
    assert response.status_code == 202


def test_ldap_get_all_converted_endpoint(
    test_client: TestClient, headers: dict
) -> None:
    """Test the LDAP get-all endpoint on our app."""

    response = test_client.get("/LDAP/Employee/converted", headers=headers)
    assert response.status_code == 202


def test_ldap_get_converted_endpoint(test_client: TestClient, headers: dict) -> None:
    """Test the LDAP get endpoint on our app."""

    response = test_client.get("/LDAP/Employee/010101-1234/converted", headers=headers)
    assert response.status_code == 202

    response = test_client.get("/LDAP/Employee/invalid_cpr/converted", headers=headers)
    assert response.status_code == 422


def test_ldap_post_ldap_employee_endpoint(
    test_client: TestClient, headers: dict
) -> None:
    """Test the LDAP get-all endpoint on our app."""

    ldap_person_to_post = {
        "dn": "CN=Lars Peter Thomsen,OU=Users,OU=Magenta,DC=ad,DC=addev",
        "cpr": "0101121234",
        "name": "Lars Peter Thomsen",
        "Department": None,
    }
    response = test_client.post(
        "/LDAP/Employee", json=ldap_person_to_post, headers=headers
    )
    assert response.status_code == 200


def test_mo_get_employee_endpoint(test_client: TestClient, headers: dict) -> None:
    """Test the MO get-all endpoint on our app."""

    uuid = uuid4()

    response = test_client.get(f"/MO/Employee/{uuid}", headers=headers)
    assert response.status_code == 202


def test_mo_post_employee_endpoint(test_client: TestClient, headers: dict) -> None:
    """Test the MO get-all endpoint on our app."""

    employee_to_post = {
        "uuid": "ff5bfef4-6459-4ba2-9571-10366ead6f5f",
        "user_key": "ff5bfef4-6459-4ba2-9571-10366ead6f5f",
        "type": "employee",
        "givenname": "Jens Pedersen Munch",
        "surname": "Bisgaard",
        "name": None,
        "cpr_no": "0910443755",
        "seniority": None,
        "org": None,
        "nickname_givenname": "Man who can do 6571 push ups",
        "nickname_surname": "Superman",
        "nickname": None,
        "details": None,
    }

    response = test_client.post("/MO/Employee", json=employee_to_post, headers=headers)
    assert response.status_code == 200


def test_ldap_get_organizationalUser_endpoint(
    test_client: TestClient, headers: dict
) -> None:
    """Test the LDAP get endpoint on our app."""

    response = test_client.get("/LDAP/Employee/010101-1234", headers=headers)
    assert response.status_code == 202

    response = test_client.get("/LDAP/Employee/invalid_cpr", headers=headers)
    assert response.status_code == 422


def test_ldap_get_overview_endpoint(test_client: TestClient, headers: dict) -> None:
    """Test the LDAP get endpoint on our app."""

    response = test_client.get("/Inspect/overview", headers=headers)
    assert response.status_code == 202


def test_ldap_get_populated_overview_endpoint(
    test_client: TestClient, headers: dict
) -> None:
    """Test the LDAP get endpoint on our app."""

    response = test_client.get("/Inspect/overview/populated", headers=headers)
    assert response.status_code == 202


def test_ldap_get_attribute_details_endpoint(
    test_client: TestClient, headers: dict
) -> None:
    """Test the LDAP get endpoint on our app."""

    response = test_client.get("/Inspect/attribute/foo", headers=headers)
    assert response.status_code == 202


def test_ldap_get_object_endpoint(test_client: TestClient, headers: dict) -> None:
    """Test the LDAP get endpoint on our app."""

    uuid = uuid4()
    response = test_client.get(f"/Inspect/object/{uuid}", headers=headers)
    assert response.status_code == 202


async def test_listen_to_changes(
    load_settings_overrides: dict[str, str], dataloader: AsyncMock, sync_tool: AsyncMock
):

    context = {"user_context": {"dataloader": dataloader, "sync_tool": sync_tool}}
    payload = MagicMock()
    payload.uuid = uuid4()
    payload.object_uuid = uuid4()

    mo_routing_key = MORoutingKey.build("employee.*.*")
    await listen_to_changes(context, payload, mo_routing_key=mo_routing_key)
    sync_tool.listen_to_changes_in_employees.assert_awaited_once()

    mo_routing_key = MORoutingKey.build("org_unit.*.*")
    await listen_to_changes(context, payload, mo_routing_key=mo_routing_key)
    sync_tool.listen_to_changes_in_org_units.assert_awaited_once()


async def test_listen_to_changes_not_listening(
    load_settings_overrides_not_listening: dict[str, str]
) -> None:

    mo_routing_key = MORoutingKey.build("employee.employee.edit")
    context: dict = {}
    payload = MagicMock()

    with pytest.raises(RejectMessage):
        await asyncio.gather(
            listen_to_changes(context, payload, mo_routing_key=mo_routing_key),
        )


def test_ldap_get_all_converted_endpoint_failure(
    test_client: TestClient, converter: MagicMock, headers: dict
) -> None:
    def from_ldap(ldap_object, json_key, employee_uuid=None):
        # This will raise a validationError because the ldap_object is not converted
        return Employee(**ldap_object.dict())

    converter.from_ldap = from_ldap
    with patch("mo_ldap_import_export.main.LdapConverter", return_value=converter):
        response1 = test_client.get("/LDAP/Employee/converted", headers=headers)
        response2 = test_client.get(
            "/LDAP/Employee/010101-1234/converted", headers=headers
        )

    assert response1.status_code == 202
    assert response2.status_code == 404


def test_load_address_from_MO_endpoint(test_client: TestClient, headers: dict):
    uuid = uuid4()
    response = test_client.get(f"/MO/Address/{uuid}", headers=headers)
    assert response.status_code == 202


def test_load_address_types_from_MO_endpoint(test_client: TestClient, headers: dict):
    response = test_client.get("/MO/Address_types", headers=headers)
    assert response.status_code == 202


def test_load_it_systems_from_MO_endpoint(test_client: TestClient, headers: dict):
    response = test_client.get("/MO/IT_systems", headers=headers)
    assert response.status_code == 202


def test_reload_info_dicts_endpoint(test_client: TestClient, headers: dict):
    response = test_client.post("/reload_info_dicts", headers=headers)
    assert response.status_code == 202


def test_load_primary_types_from_MO_endpoint(test_client: TestClient, headers: dict):
    response = test_client.get("/MO/Primary_types", headers=headers)
    assert response.status_code == 202


async def test_import_all_objects_from_LDAP_first_20(
    test_client: TestClient, headers: dict
) -> None:
    params = {
        "test_on_first_20_entries": True,
        "delay_in_hours": 0,
        "delay_in_minutes": 0,
        "delay_in_seconds": 0.1,
    }
    response = test_client.get("/Import", headers=headers, params=params)
    assert response.status_code == 202


async def test_import_all_objects_from_LDAP(
    test_client: TestClient, headers: dict
) -> None:
    response = test_client.get("/Import", headers=headers)
    assert response.status_code == 202


async def test_import_one_object_from_LDAP(
    test_client: TestClient, headers: dict
) -> None:
    uuid = uuid4()
    response = test_client.get(f"/Import/{uuid}", headers=headers)
    assert response.status_code == 202


async def test_import_all_objects_from_LDAP_no_cpr_field(
    converter: MagicMock,
    test_client_no_cpr: TestClient,
    headers: dict,
) -> None:
    response = test_client_no_cpr.get("/Import", headers=headers)
    assert response.status_code == 404


async def test_import_all_objects_from_LDAP_invalid_cpr(
    test_client: TestClient, headers: dict, dataloader: AsyncMock
) -> None:
    dataloader.load_ldap_objects.return_value = [
        LdapObject(name="Tester", Department="QA", dn="someDN", EmployeeID="5001012002")
    ]

    with capture_logs() as cap_logs:
        response = test_client.get("/Import", headers=headers)
        assert response.status_code == 202

        messages = [w for w in cap_logs if w["log_level"] == "info"]
        assert re.match(
            ".*not a valid cpr number",
            str(messages[-1]["event"]),
        )


async def test_load_mapping_file_environment(
    load_settings_overrides_incorrect_mapping: dict[str, str],
    disable_metrics: None,
    converter: MagicMock,
) -> None:

    with patch(
        "mo_ldap_import_export.main.configure_ldap_connection", new_callable=MagicMock()
    ), patch(
        "mo_ldap_import_export.main.LdapConverter", return_value=converter
    ), pytest.raises(
        FileNotFoundError
    ):
        fastramqpi = create_fastramqpi()
        assert isinstance(fastramqpi, FastRAMQPI)


async def test_load_faulty_username_generator(
    disable_metrics: None,
    load_settings_overrides: dict[str, str],
    gql_client: AsyncMock,
    dataloader: AsyncMock,
    converter: MagicMock,
) -> None:

    usernames_mock = MagicMock()
    usernames_mock.UserNameGenerator.return_value = "foo"

    with patch(
        "mo_ldap_import_export.main.configure_ldap_connection", new_callable=MagicMock()
    ), patch(
        "mo_ldap_import_export.main.construct_gql_client",
        return_value=gql_client,
    ), patch(
        "mo_ldap_import_export.main.DataLoader", return_value=dataloader
    ), patch(
        "mo_ldap_import_export.main.LdapConverter", return_value=converter
    ), patch(
        "mo_ldap_import_export.main.usernames", usernames_mock
    ), pytest.raises(
        AttributeError
    ):
        fastramqpi = create_fastramqpi()
        assert isinstance(fastramqpi, FastRAMQPI)


def test_invalid_credentials(test_client: TestClient):
    response = test_client.post(
        "/login", data={"username": "admin", "password": "wrong_password"}
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid credentials"


def test_invalid_username(test_client: TestClient):
    response = test_client.post(
        "/login", data={"username": "wrong_username", "password": "admin"}
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid credentials"


async def test_synchronize_todays_events(
    test_client: TestClient,
    headers: dict,
    internal_amqpsystem: AsyncMock,
    test_mo_objects: list,
):
    today = datetime.datetime.today().strftime("%Y-%m-%d")
    json = {
        "date": today,
        "publish_amqp_messages": True,
    }
    response = test_client.post(
        "/Synchronize_todays_events", headers=headers, json=json
    )
    assert response.status_code == 202

    n = 0
    for mo_object in test_mo_objects:
        payload = jsonable_encoder(mo_object["payload"])

        from_date = str(mo_object["validity"]["from"])
        to_date = str(mo_object["validity"]["to"])

        if from_date.startswith(today):
            if to_date.startswith(today):
                routing_key = "employee.employee.terminate"
            else:
                routing_key = "employee.employee.refresh"
        elif to_date.startswith(today):
            routing_key = "employee.employee.terminate"
        else:
            routing_key = None

        if routing_key:
            internal_amqpsystem.publish_message.assert_any_await(routing_key, payload)
            n += 1

    assert internal_amqpsystem.publish_message.await_count == n

    # Test that terminations are published before refreshes
    refreshes = 0
    terminations = 0
    for call in internal_amqpsystem.publish_message.mock_calls:
        if "terminate" in call.args[0]:
            terminations += 1
            assert refreshes == 0
        else:
            refreshes += 1


async def test_export_endpoint(
    test_client: TestClient,
    headers: dict,
    internal_amqpsystem: AsyncMock,
    test_mo_objects: list,
):

    params = {
        "publish_amqp_messages": True,
        "uuid": str(uuid4()),
        "delay_in_hours": 0,
        "delay_in_minutes": 0,
        "delay_in_seconds": 0.1,
    }

    response = test_client.post("/Export", headers=headers, params=params)
    assert response.status_code == 202

    for mo_object in test_mo_objects:
        payload = jsonable_encoder(mo_object["payload"])
        internal_amqpsystem.publish_message.assert_any_await(
            "employee.employee.refresh", payload
        )

    assert internal_amqpsystem.publish_message.await_count == len(test_mo_objects)


async def test_reject_on_failure():
    async def not_supported_func():
        raise NotSupportedException("")

    async def incorrect_mapping_func():
        raise IncorrectMapping("")

    async def transport_query_error_func():
        raise TransportQueryError("")

    async def no_objects_returned_func():
        raise NoObjectsReturnedException("")

    async def type_error_func():
        raise TypeError("")

    # These exceptions should result in rejectMessage exceptions()
    for func in [
        not_supported_func,
        incorrect_mapping_func,
        transport_query_error_func,
        no_objects_returned_func,
    ]:
        with pytest.raises(RejectMessage):
            await reject_on_failure(func)()

    # But not this one
    with patch("mo_ldap_import_export.main.delay_on_error", 0.5):
        with pytest.raises(TypeError):
            await reject_on_failure(type_error_func)()


async def test_get_delete_flag(dataloader: AsyncMock):

    payload = PayloadType(
        uuid=uuid4(),
        object_uuid=uuid4(),
        time=datetime.datetime.now(),
    )

    # When the routing key != TERMINATE, do not delete anything
    routing_key = MORoutingKey.build(
        service_type=ServiceType.EMPLOYEE,
        object_type=ObjectType.EMPLOYEE,
        request_type=RequestType.REFRESH,
    )
    dataloader.load_mo_object.return_value = None
    context = Context({"user_context": {"dataloader": dataloader}})
    flag = await asyncio.gather(get_delete_flag(routing_key, payload, context))
    assert flag == [False]

    # When there are no matching objects in MO any longer, delete
    routing_key = MORoutingKey.build(
        service_type=ServiceType.EMPLOYEE,
        object_type=ObjectType.EMPLOYEE,
        request_type=RequestType.TERMINATE,
    )
    dataloader.load_mo_object.return_value = None
    context = Context({"user_context": {"dataloader": dataloader}})
    flag = await asyncio.gather(get_delete_flag(routing_key, payload, context))
    assert flag == [True]

    # When there are matching objects in MO, but the to-date is today, delete
    dataloader.load_mo_object.return_value = {
        "validity": {"to": datetime.datetime.today().strftime("%Y-%m-%d")}
    }

    context = Context({"user_context": {"dataloader": dataloader}})
    flag = await asyncio.gather(get_delete_flag(routing_key, payload, context))
    assert flag == [True]

    # When there are matching objects in MO, but the to-date is not today, abort
    dataloader.load_mo_object.return_value = {"validity": {"to": "2200-01-01"}}
    context = Context({"user_context": {"dataloader": dataloader}})
    with pytest.raises(RejectMessage):
        await asyncio.gather(get_delete_flag(routing_key, payload, context))


def test_get_invalid_cpr_numbers_from_LDAP_endpoint(
    test_client: TestClient,
    headers: dict,
    dataloader: AsyncMock,
):
    valid_object = LdapObject(dn="foo", EmployeeID="0101011234")
    invalid_object = LdapObject(dn="bar", EmployeeID="ja")
    dataloader.load_ldap_objects.return_value = [valid_object, invalid_object]
    response = test_client.get("/Inspect/invalid_cpr_numbers", headers=headers)
    assert response.status_code == 202
    result = response.json()
    assert "bar" in result
    assert result["bar"] == "ja"


def test_get_invalid_cpr_numbers_from_LDAP_endpoint_no_cpr_field(
    test_client_no_cpr: TestClient,
    headers: dict,
    dataloader: AsyncMock,
):
    response = test_client_no_cpr.get("/Inspect/invalid_cpr_numbers", headers=headers)
    assert response.status_code == 404


def test_wraps():
    """
    Test that the decorated listen_to_changes function keeps its name
    """
    assert listen_to_changes.__name__ == "listen_to_changes"
