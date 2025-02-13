# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Event handling."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from contextlib import asynccontextmanager
from typing import Annotated
from typing import Any
from uuid import UUID

import structlog
from fastapi import APIRouter
from fastapi import Body
from fastapi import Depends
from fastapi import FastAPI
from fastramqpi.main import FastRAMQPI
from fastramqpi.ramqp import AMQPSystem
from fastramqpi.ramqp.depends import handle_exclusively_decorator
from fastramqpi.ramqp.depends import rate_limit
from fastramqpi.ramqp.mo import MORouter
from fastramqpi.ramqp.mo import PayloadUUID
from fastramqpi.ramqp.utils import RejectMessage
from fastramqpi.ramqp.utils import RequeueMessage
from ldap3 import Connection
from more_itertools import one

from mo_ldap_import_export.ldapapi import LDAPAPI
from mo_ldap_import_export.moapi import MOAPI

from . import depends
from .autogenerated_graphql_client import GraphQLClient
from .config import Settings
from .converters import LdapConverter
from .customer_specific_checks import ExportChecks
from .customer_specific_checks import ImportChecks
from .database import Base
from .dataloaders import DataLoader
from .exceptions import amqp_reject_on_failure
from .exceptions import http_reject_on_failure
from .import_export import SyncTool
from .ldap import check_ou_in_list_of_ous
from .ldap import configure_ldap_connection
from .ldap import ldap_healthcheck
from .ldap_amqp import configure_ldap_amqpsystem
from .ldap_amqp import ldap2mo_router
from .ldap_event_generator import LDAPEventGenerator
from .ldap_event_generator import ldap_event_router
from .routes import construct_router
from .usernames import get_username_generator_class

logger = structlog.stdlib.get_logger()

amqp_router = MORouter()
mo2ldap_router = APIRouter(prefix="/mo2ldap")


@mo2ldap_router.post("/address")
@http_reject_on_failure
async def http_process_address(
    object_uuid: Annotated[UUID, Body()],
    graphql_client: depends.GraphQLClient,
    amqpsystem: depends.AMQPSystem,
) -> None:
    await handle_address(object_uuid, graphql_client, amqpsystem)


@amqp_router.register("address")
@amqp_reject_on_failure
async def process_address(
    object_uuid: PayloadUUID,
    graphql_client: depends.GraphQLClient,
    amqpsystem: depends.AMQPSystem,
) -> None:
    await handle_address(object_uuid, graphql_client, amqpsystem)


async def handle_address(
    object_uuid: UUID,
    graphql_client: depends.GraphQLClient,
    amqpsystem: depends.AMQPSystem,
) -> None:
    result = await graphql_client.read_address_relation_uuids(object_uuid)
    try:
        obj = one(result.objects)
    except ValueError as error:
        logger.warning("Unable to lookup address", uuid=object_uuid)
        raise RejectMessage("Unable to lookup address") from error

    if obj.current is None:
        logger.warning("Address not currently active", uuid=object_uuid)
        raise RejectMessage("Address not currently active")

    person_uuid = obj.current.employee_uuid
    org_unit_uuid = obj.current.org_unit_uuid

    if person_uuid is not None:
        # TODO: Add support for refreshing persons with a certain address directly
        await graphql_client.employee_refresh(amqpsystem.exchange_name, [person_uuid])
    if org_unit_uuid is not None:
        # TODO: Should really only be primary engagement relations
        e_result = await graphql_client.read_employees_with_engagement_to_org_unit(
            org_unit_uuid
        )
        employee_uuids = {
            x.current.employee_uuid for x in e_result.objects if x.current is not None
        }
        # TODO: Add support for refreshing persons with a primary engagement relation directly
        await graphql_client.employee_refresh(
            amqpsystem.exchange_name, list(employee_uuids)
        )


@mo2ldap_router.post("/engagement")
@http_reject_on_failure
async def http_process_engagement(
    object_uuid: Annotated[UUID, Body()],
    graphql_client: depends.GraphQLClient,
    amqpsystem: depends.AMQPSystem,
) -> None:
    await handle_engagement(object_uuid, graphql_client, amqpsystem)


@amqp_router.register("engagement")
@amqp_reject_on_failure
async def process_engagement(
    object_uuid: PayloadUUID,
    graphql_client: depends.GraphQLClient,
    amqpsystem: depends.AMQPSystem,
) -> None:
    await handle_engagement(object_uuid, graphql_client, amqpsystem)


async def handle_engagement(
    object_uuid: UUID,
    graphql_client: depends.GraphQLClient,
    amqpsystem: depends.AMQPSystem,
) -> None:
    result = await graphql_client.read_engagement_employee_uuid(object_uuid)
    try:
        obj = one(result.objects)
    except ValueError as error:
        logger.warning("Unable to lookup engagement", uuid=object_uuid)
        raise RejectMessage("Unable to lookup engagement") from error

    if obj.current is None:
        logger.warning("Engagement not currently active", uuid=object_uuid)
        raise RejectMessage("Engagement not currently active")

    person_uuid = obj.current.employee_uuid
    # TODO: Add support for refreshing persons with a certain engagement directly
    await graphql_client.employee_refresh(amqpsystem.exchange_name, [person_uuid])


@mo2ldap_router.post("/ituser")
@http_reject_on_failure
async def http_process_ituser(
    object_uuid: Annotated[UUID, Body()],
    graphql_client: depends.GraphQLClient,
    amqpsystem: depends.AMQPSystem,
) -> None:
    await handle_ituser(object_uuid, graphql_client, amqpsystem)


@amqp_router.register("ituser")
@amqp_reject_on_failure
async def process_ituser(
    object_uuid: PayloadUUID,
    graphql_client: depends.GraphQLClient,
    amqpsystem: depends.AMQPSystem,
) -> None:
    await handle_ituser(object_uuid, graphql_client, amqpsystem)


async def handle_ituser(
    object_uuid: UUID,
    graphql_client: depends.GraphQLClient,
    amqpsystem: depends.AMQPSystem,
) -> None:
    result = await graphql_client.read_ituser_employee_uuid(object_uuid)
    try:
        obj = one(result.objects)
    except ValueError as error:
        logger.warning("Unable to lookup ITUser", uuid=object_uuid)
        raise RejectMessage("Unable to lookup ITUser") from error

    if obj.current is None:
        logger.warning("ITUser not currently active", uuid=object_uuid)
        raise RejectMessage("ITUser not currently active")

    person_uuid = obj.current.employee_uuid
    if person_uuid is None:
        logger.warning("ITUser not attached to a person", uuid=object_uuid)
        raise RejectMessage("ITUser not attached to a person")

    # TODO: Add support for refreshing persons with a certain ituser directly
    await graphql_client.employee_refresh(amqpsystem.exchange_name, [person_uuid])


@mo2ldap_router.post("/person")
@handle_exclusively_decorator(key=lambda object_uuid, *_, **__: object_uuid)
@http_reject_on_failure
async def http_process_person(
    object_uuid: Annotated[UUID, Body()],
    sync_tool: depends.SyncTool,
) -> None:
    await sync_tool.listen_to_changes_in_employees(object_uuid)


@amqp_router.register("person")
@handle_exclusively_decorator(key=lambda object_uuid, *_, **__: object_uuid)
async def process_person(
    object_uuid: PayloadUUID,
    sync_tool: depends.SyncTool,
    graphql_client: depends.GraphQLClient,
    amqpsystem: depends.AMQPSystem,
) -> None:
    try:
        await amqp_reject_on_failure(sync_tool.listen_to_changes_in_employees)(
            object_uuid
        )
    except RequeueMessage:  # pragma: no cover
        # NOTE: This is a hack to cycle messages because quorum queues do not work
        await asyncio.sleep(30)
        await graphql_client.employee_refresh(amqpsystem.exchange_name, [object_uuid])


@mo2ldap_router.post("/org_unit")
@http_reject_on_failure
async def http_process_org_unit(
    object_uuid: Annotated[UUID, Body()],
    graphql_client: depends.GraphQLClient,
    amqpsystem: depends.AMQPSystem,
) -> None:
    await handle_org_unit(object_uuid, graphql_client, amqpsystem)


@amqp_router.register("org_unit")
@amqp_reject_on_failure
async def process_org_unit(
    object_uuid: PayloadUUID,
    graphql_client: depends.GraphQLClient,
    amqpsystem: depends.AMQPSystem,
) -> None:
    await handle_org_unit(object_uuid, graphql_client, amqpsystem)


async def handle_org_unit(
    object_uuid: UUID,
    graphql_client: GraphQLClient,
    amqpsystem: AMQPSystem,
) -> None:
    logger.info(
        "Registered change in an org_unit",
        object_uuid=object_uuid,
    )
    # In case the name of the org-unit changed, we need to publish an
    # "engagement" message for each of its employees. Because org-unit
    # LDAP mapping is primarily done through the "Engagement" json-key.
    await graphql_client.org_unit_engagements_refresh(
        amqpsystem.exchange_name, object_uuid
    )


@asynccontextmanager
async def open_ldap_connection(ldap_connection: Connection) -> AsyncIterator[None]:
    """Open the LDAP connection during FastRAMQPI lifespan.

    Yields:
        None
    """
    with ldap_connection:
        yield


@asynccontextmanager
async def lifespan(
    fastramqpi: FastRAMQPI,
    settings: Settings,
) -> AsyncIterator[None]:
    async with AsyncExitStack() as stack:
        logger.info("Configuring LDAP connection")
        ldap_connection = configure_ldap_connection(settings)
        fastramqpi.add_context(ldap_connection=ldap_connection)
        fastramqpi.add_healthcheck(name="LDAPConnection", healthcheck=ldap_healthcheck)
        await stack.enter_async_context(open_ldap_connection(ldap_connection))

        context = fastramqpi.get_context()
        graphql_client: GraphQLClient = context["graphql_client"]

        logger.info("Initializing MOAPI")
        moapi = MOAPI(settings, graphql_client)

        logger.info("Initializing LDAPAPI")
        ldapapi = LDAPAPI(settings, ldap_connection)

        logger.info("Initializing username generator")
        username_generator_class = get_username_generator_class(
            settings.conversion_mapping.username_generator.objectClass
        )
        username_generator = username_generator_class(
            settings, moapi, ldapapi.ldap_connection
        )

        logger.info("Initializing dataloader")
        dataloader = DataLoader(settings, moapi, ldapapi, username_generator)
        fastramqpi.add_context(dataloader=dataloader)

        logger.info("Initializing Import/Export checks")
        export_checks = ExportChecks(dataloader)
        import_checks = ImportChecks()

        logger.info("Initializing converters")
        converter = LdapConverter(settings, dataloader)
        fastramqpi.add_context(converter=converter)

        logger.info("Initializing Sync tool")
        sync_tool = SyncTool(
            dataloader,
            converter,
            export_checks,
            import_checks,
            settings,
            ldap_connection,
        )
        fastramqpi.add_context(sync_tool=sync_tool)

        logger.info("Starting AMQP listener")
        amqpsystem = fastramqpi.get_amqpsystem()
        await stack.enter_async_context(amqpsystem)

        logger.info("Initializing LDAP listener")
        ldap_amqpsystem = configure_ldap_amqpsystem(fastramqpi, settings)
        await stack.enter_async_context(ldap_amqpsystem)
        if settings.listen_to_changes_in_ldap:
            logger.info("Initializing LDAP event generator")
            sessionmaker = fastramqpi.get_context()["sessionmaker"]
            ldap_event_generator = LDAPEventGenerator(
                sessionmaker, settings, ldap_amqpsystem, ldap_connection
            )
            fastramqpi.add_healthcheck(
                name="LDAPEventGenerator", healthcheck=ldap_event_generator.healthcheck
            )
            await stack.enter_async_context(ldap_event_generator)

        logger.info("Starting program")
        yield


def create_fastramqpi(**kwargs: Any) -> FastRAMQPI:
    """FastRAMQPI factory.

    Returns:
        FastRAMQPI system.
    """
    logger.info("Retrieving settings")
    settings = Settings(**kwargs)

    # ldap_ou_for_new_users needs to be in the search base. Otherwise we cannot
    # find newly created users...
    check_ou_in_list_of_ous(
        settings.ldap_ou_for_new_users,
        settings.ldap_ous_to_search_in,
    )

    # We also need to check for permission to write to this OU
    check_ou_in_list_of_ous(
        settings.ldap_ou_for_new_users,
        settings.ldap_ous_to_write_to,
    )

    logger.info("Setting up FastRAMQPI")
    fastramqpi = FastRAMQPI(
        application_name="ldap_ie",
        settings=settings.fastramqpi,
        graphql_version=25,
        graphql_client_cls=GraphQLClient,
        database_metadata=Base.metadata,
    )
    fastramqpi.add_context(settings=settings)

    logger.info("AMQP router setup")
    amqpsystem = fastramqpi.get_amqpsystem()
    # Retry messages after a short period of time
    rate_limit_delay = 10
    amqpsystem.dependencies = [
        Depends(rate_limit(rate_limit_delay)),
        Depends(depends.logger_bound_message_id),
        Depends(depends.request_id),
    ]
    if settings.listen_to_changes_in_mo:
        amqpsystem.router.registry.update(amqp_router.registry)

    # We delay AMQPSystem start, to detect it from client startup
    # TODO: This separation should probably be in FastRAMQPI
    priority_set = fastramqpi._context["lifespan_managers"][1000]
    priority_set.remove(amqpsystem)

    fastramqpi.add_lifespan_manager(lifespan(fastramqpi, settings), 2000)

    return fastramqpi


def create_app(fastramqpi: FastRAMQPI | None = None, **kwargs: Any) -> FastAPI:
    """FastAPI application factory.

    Returns:
        FastAPI application.
    """
    if fastramqpi is None:
        fastramqpi = create_fastramqpi(**kwargs)
    assert fastramqpi is not None

    app = fastramqpi.get_app()
    settings = fastramqpi._context["user_context"]["settings"]
    app.include_router(construct_router(settings))
    app.include_router(mo2ldap_router)
    app.include_router(ldap2mo_router)
    app.include_router(ldap_event_router)

    return app
