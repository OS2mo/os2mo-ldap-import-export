# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
"""Event handling."""
import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from functools import wraps
from inspect import iscoroutinefunction
from typing import Annotated
from typing import Any

from fastapi import APIRouter
from fastapi import Depends
from fastapi import FastAPI
from fastapi_utils.tasks import repeat_every
from fastramqpi.main import FastRAMQPI
from fastramqpi.ramqp import AMQPSystem
from fastramqpi.ramqp.depends import Context
from fastramqpi.ramqp.depends import rate_limit
from fastramqpi.ramqp.mo import MORouter
from fastramqpi.ramqp.mo import MORoutingKey
from fastramqpi.ramqp.mo import PayloadUUID
from fastramqpi.ramqp.utils import RejectMessage
from fastramqpi.ramqp.utils import RequeueMessage
from gql.transport.exceptions import TransportQueryError
from ldap3 import Connection

from . import depends
from . import usernames
from .autogenerated_graphql_client import GraphQLClient
from .config import Settings
from .converters import LdapConverter
from .customer_specific_checks import ExportChecks
from .customer_specific_checks import ImportChecks
from .dataloaders import DataLoader
from .exceptions import IgnoreChanges
from .exceptions import IncorrectMapping
from .exceptions import NoObjectsReturnedException
from .exceptions import NotEnabledException
from .exceptions import NotSupportedException
from .import_export import SyncTool
from .ldap import check_ou_in_list_of_ous
from .ldap import configure_ldap_connection
from .ldap import ldap_healthcheck
from .ldap import poller_healthcheck
from .ldap import setup_listener
from .ldap_amqp import configure_ldap_amqpsystem
from .logging import logger
from .os2mo_init import InitEngine
from .routes import construct_router
from .utils import get_object_type_from_routing_key
from .utils import mo_datestring_to_utc

fastapi_router = APIRouter()
amqp_router = MORouter()
internal_amqp_router = MORouter()
delay_on_error = 10  # Try errors again after a short period of time
delay_on_requeue = 60 * 60 * 24  # Requeue messages for tomorrow (or after a reboot)
RateLimit = Annotated[None, Depends(rate_limit(delay_on_error))]


def reject_on_failure(func):
    """
    Decorator to turn message into dead letter in case of exceptions.
    """

    @wraps(func)
    async def modified_func(*args, **kwargs):
        try:
            await func(*args, **kwargs)
        except (
            NotSupportedException,  # For features that are not supported: Abort
            IncorrectMapping,  # If the json dict is incorrectly configured: Abort
            TransportQueryError,  # In case an ldap entry cannot be uploaded: Abort
            NoObjectsReturnedException,  # In case an object is deleted halfway: Abort
            IgnoreChanges,  # In case changes should be ignored: Abort
            RejectMessage,  # In case we explicitly reject the message: Abort
            NotEnabledException,  # In case a feature is not enabled: Abort
        ) as e:
            logger.info(e)
            raise RejectMessage()
        except RequeueMessage:
            await asyncio.sleep(delay_on_requeue)
            raise

    modified_func.__wrapped__ = func  # type: ignore
    return modified_func


def get_delete_flag(mo_object: dict[str, Any]) -> bool:
    """
    Determines if an object should be deleted based on the validity to-date
    """
    now = datetime.utcnow()
    validity_to = mo_datestring_to_utc(mo_object["validity"]["to"])
    if validity_to and validity_to <= now:
        logger.info(
            "[Get-delete-flag] Returning delete=True because "
            f"to-date ({validity_to}) <= current date ({now})"
        )
        return True
    return False


async def unpack_payload(
    context: Context, object_uuid: PayloadUUID, mo_routing_key: MORoutingKey
) -> tuple[dict[Any, Any], Any]:
    """
    Takes the payload of an AMQP message, and returns a set of parameters to be used
    by export functions in `import_export.py`. Also return the mo object as a dict
    """

    # If we are not supposed to listen: reject and turn the message into a dead letter.
    settings = context["user_context"]["settings"]
    if not settings.listen_to_changes_in_mo:
        logger.info("[Unpack-payload] listen_to_changes_in_mo = False. Aborting.")
        raise RejectMessage()

    logger.info(
        "[Unpack-payload] Unpacking payload.",
        mo_routing_key=mo_routing_key,
        object_uuid=str(object_uuid),
    )

    dataloader: DataLoader = context["user_context"]["dataloader"]

    object_type = get_object_type_from_routing_key(mo_routing_key)

    mo_object = await dataloader.load_mo_object(
        str(object_uuid),
        object_type,
        add_validity=True,
        current_objects_only=False,
    )
    if mo_object is None:
        raise RejectMessage("Unable to load mo object")

    delete = get_delete_flag(mo_object)
    current_objects_only = False if delete else True

    args = dict(
        uuid=mo_object["parent_uuid"],
        object_uuid=object_uuid,
        routing_key=mo_routing_key,
        delete=delete,
        current_objects_only=current_objects_only,
    )

    return args, mo_object


@internal_amqp_router.register("address")
@amqp_router.register("address")
@reject_on_failure
async def process_address(
    context: Context,
    object_uuid: PayloadUUID,
    mo_routing_key: MORoutingKey,
    sync_tool: depends.SyncTool,
    _: RateLimit,
) -> None:
    args, mo_object = await unpack_payload(context, object_uuid, mo_routing_key)
    service_type = mo_object["service_type"]

    if service_type == "employee":
        await sync_tool.listen_to_changes_in_employees(**args)
    elif service_type == "org_unit":
        await sync_tool.listen_to_changes_in_org_units(**args)


@internal_amqp_router.register("engagement")
@amqp_router.register("engagement")
@reject_on_failure
async def process_engagement(
    context: Context,
    object_uuid: PayloadUUID,
    mo_routing_key: MORoutingKey,
    sync_tool: depends.SyncTool,
    _: RateLimit,
) -> None:
    args, _ = await unpack_payload(context, object_uuid, mo_routing_key)

    await sync_tool.listen_to_changes_in_employees(**args)
    await sync_tool.export_org_unit_addresses_on_engagement_change(**args)


@internal_amqp_router.register("ituser")
@amqp_router.register("ituser")
@reject_on_failure
async def process_ituser(
    context: Context,
    object_uuid: PayloadUUID,
    mo_routing_key: MORoutingKey,
    sync_tool: depends.SyncTool,
    _: RateLimit,
) -> None:
    args, _ = await unpack_payload(context, object_uuid, mo_routing_key)

    await sync_tool.listen_to_changes_in_employees(**args)


@internal_amqp_router.register("person")
@amqp_router.register("person")
@reject_on_failure
async def process_person(
    context: Context,
    object_uuid: PayloadUUID,
    mo_routing_key: MORoutingKey,
    sync_tool: depends.SyncTool,
    _: RateLimit,
) -> None:
    args, _ = await unpack_payload(context, object_uuid, mo_routing_key)

    await sync_tool.listen_to_changes_in_employees(**args)


@internal_amqp_router.register("org_unit")
@amqp_router.register("org_unit")
@reject_on_failure
async def process_org_unit(
    context: Context,
    object_uuid: PayloadUUID,
    mo_routing_key: MORoutingKey,
    sync_tool: depends.SyncTool,
    _: RateLimit,
) -> None:
    args, _ = await unpack_payload(context, object_uuid, mo_routing_key)

    await sync_tool.listen_to_changes_in_org_units(**args)


@asynccontextmanager
async def open_ldap_connection(ldap_connection: Connection) -> AsyncIterator[None]:
    """Open the LDAP connection during FastRAMQPI lifespan.

    Yields:
        None
    """
    with ldap_connection:
        yield


# https://fastapi.tiangolo.com/advanced/events/
@asynccontextmanager
async def initialize_sync_tool(fastramqpi: FastRAMQPI) -> AsyncIterator[None]:
    logger.info("Initializing Sync tool")
    sync_tool = SyncTool(fastramqpi.get_context())
    fastramqpi.add_context(sync_tool=sync_tool)
    yield


@asynccontextmanager
async def initialize_checks(fastramqpi: FastRAMQPI) -> AsyncIterator[None]:
    logger.info("Initializing Import/Export checks")
    export_checks = ExportChecks(fastramqpi.get_context())
    import_checks = ImportChecks(fastramqpi.get_context())
    fastramqpi.add_context(export_checks=export_checks, import_checks=import_checks)
    yield


@asynccontextmanager
async def initialize_converters(fastramqpi: FastRAMQPI) -> AsyncIterator[None]:
    logger.info("Initializing converters")
    converter = LdapConverter(fastramqpi.get_context())
    await converter._init()
    fastramqpi.add_context(cpr_field=converter.cpr_field)
    fastramqpi.add_context(ldap_it_system_user_key=converter.ldap_it_system)
    fastramqpi.add_context(converter=converter)
    yield


@asynccontextmanager
async def initialize_init_engine(fastramqpi: FastRAMQPI) -> AsyncIterator[None]:
    logger.info("Initializing os2mo-init engine")
    init_engine = InitEngine(fastramqpi.get_context())
    await init_engine.create_facets()
    await init_engine.create_it_systems()
    fastramqpi.add_context(init_engine=init_engine)
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
        graphql_version=21,
        graphql_client_cls=GraphQLClient,
    )
    fastramqpi.add_context(settings=settings)

    logger.info("AMQP router setup")
    amqpsystem = fastramqpi.get_amqpsystem()
    amqpsystem.router.registry.update(amqp_router.registry)

    logger.info("Configuring LDAP connection")
    ldap_connection = configure_ldap_connection(settings)
    fastramqpi.add_context(ldap_connection=ldap_connection)
    fastramqpi.add_healthcheck(name="LDAPConnection", healthcheck=ldap_healthcheck)
    fastramqpi.add_lifespan_manager(
        open_ldap_connection(ldap_connection),  # type: ignore
        1500,
    )

    logger.info("Loading mapping file")
    mapping = settings.conversion_mapping.dict(exclude_unset=True, by_alias=True)
    fastramqpi.add_context(mapping=mapping)

    logger.info("Initializing dataloader")
    dataloader = DataLoader(fastramqpi.get_context())
    fastramqpi.add_context(dataloader=dataloader)

    userNameGeneratorClass_string = mapping["username_generator"]["objectClass"]
    logger.info(f"Importing {userNameGeneratorClass_string}")
    UserNameGenerator = getattr(usernames, userNameGeneratorClass_string)

    logger.info("Initializing username generator")
    username_generator = UserNameGenerator(fastramqpi.get_context())
    fastramqpi.add_context(username_generator=username_generator)

    if not hasattr(username_generator, "generate_dn"):
        raise AttributeError("Username generator needs to have a generate_dn function")

    if not iscoroutinefunction(getattr(username_generator, "generate_dn")):
        raise TypeError("generate_dn function needs to be a coroutine")

    fastramqpi.add_lifespan_manager(initialize_init_engine(fastramqpi), 2700)
    fastramqpi.add_lifespan_manager(initialize_converters(fastramqpi), 2800)

    logger.info("Initializing internal AMQP system")
    internal_amqpsystem = AMQPSystem(
        settings=settings.internal_amqp,
        router=internal_amqp_router,  # type: ignore
    )
    fastramqpi.add_context(internal_amqpsystem=internal_amqpsystem)
    fastramqpi.add_lifespan_manager(internal_amqpsystem)
    internal_amqpsystem.router.registry.update(internal_amqp_router.registry)
    internal_amqpsystem.context = fastramqpi._context

    configure_ldap_amqpsystem(fastramqpi, settings.ldap_amqp)

    fastramqpi.add_lifespan_manager(initialize_checks(fastramqpi), 2900)
    fastramqpi.add_lifespan_manager(initialize_sync_tool(fastramqpi), 3000)

    logger.info("Starting LDAP listener")
    fastramqpi.add_context(event_loop=asyncio.get_event_loop())
    fastramqpi.add_context(poll_time=settings.poll_time)

    if settings.listen_to_changes_in_ldap:
        pollers = setup_listener(fastramqpi.get_context())
        fastramqpi.add_context(pollers=pollers)
        fastramqpi.add_healthcheck(name="LDAPPoller", healthcheck=poller_healthcheck)

    return fastramqpi


def create_app(**kwargs: Any) -> FastAPI:
    """FastAPI application factory.

    Returns:
        FastAPI application.
    """
    fastramqpi = create_fastramqpi(**kwargs)

    app = fastramqpi.get_app()
    app.include_router(fastapi_router)

    user_context = fastramqpi._context["user_context"]

    # TODO: Eliminate this function and make reloading dicts eventdriven
    #       When this method is eliminated the fastapi_utils package can be removed
    @app.on_event("startup")
    @repeat_every(seconds=60 * 60 * 24)
    async def reload_info_dicts() -> None:  # pragma: no cover
        """
        Endpoint to reload info dicts on the converter. To make sure that they are
        up-to-date and represent the information in OS2mo.
        """
        converter = user_context["converter"]
        await converter.load_info_dicts()

    app.include_router(construct_router(user_context))

    return app
