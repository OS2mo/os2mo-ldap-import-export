# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Annotated

import structlog
from fastapi import APIRouter
from fastapi import Body
from fastapi import Depends
from fastramqpi.main import FastRAMQPI
from fastramqpi.ramqp.amqp import AMQPSystem
from fastramqpi.ramqp.amqp import Router
from fastramqpi.ramqp.depends import get_payload_as_type
from fastramqpi.ramqp.depends import rate_limit
from fastramqpi.ramqp.utils import RejectMessage

from . import depends
from .depends import DataLoader
from .depends import Settings
from .depends import SyncTool
from .depends import logger_bound_message_id
from .depends import request_id
from .exceptions import http_reject_on_failure
from .types import LDAPUUID

logger = structlog.stdlib.get_logger()


ldap2mo_router = APIRouter(prefix="/ldap2mo")

PayloadUUID = Annotated[LDAPUUID, Depends(get_payload_as_type(LDAPUUID))]


@ldap2mo_router.post("/uuid")
@http_reject_on_failure
async def http_process_uuid(
    settings: Settings,
    sync_tool: SyncTool,
    dataloader: DataLoader,
    uuid: Annotated[LDAPUUID, Body()],
) -> None:
    await handle_uuid(settings, sync_tool, dataloader, uuid)


async def handle_uuid(
    settings: Settings,
    sync_tool: SyncTool,
    dataloader: DataLoader,
    uuid: LDAPUUID,
) -> None:
    logger.info("Received LDAP AMQP event", uuid=uuid)

    if uuid in settings.ldap_uuids_to_ignore:
        logger.warning("LDAP event ignored due to ignore-list", ldap_uuid=uuid)
        return

    dn = await dataloader.ldapapi.get_ldap_dn(uuid)
    if dn is None:
        logger.error("LDAP UUID could not be found", uuid=uuid)
        raise RejectMessage("LDAP UUID could not be found")

    # Ignore changes to non-employee objects
    ldap_object_classes = await dataloader.ldapapi.get_attribute_by_dn(
        dn, "objectClass"
    )

    # TODO: Eliminate this branch by handling employees as any other object
    employee_object_class = settings.ldap_object_class
    if employee_object_class in ldap_object_classes:
        logger.info("Handling employee", ldap_object_classes=ldap_object_classes)
        await sync_tool.import_single_user(dn)

    for object_class in settings.conversion_mapping.ldap_to_mo_any:
        if object_class in ldap_object_classes:
            logger.info(
                "Handling LDAP event",
                object_class=object_class,
                ldap_object_classes=ldap_object_classes,
            )
            await sync_tool.import_single_object_class(object_class, dn)


@ldap2mo_router.post("/reconcile")
@http_reject_on_failure
async def http_reconcile_uuid(
    settings: Settings,
    dataloader: DataLoader,
    graphql_client: depends.GraphQLClient,
    uuid: Annotated[LDAPUUID, Body()],
) -> None:
    await handle_ldap_reconciliation(settings, dataloader, graphql_client, uuid)


async def handle_ldap_reconciliation(
    settings: Settings,
    dataloader: DataLoader,
    graphql_client: depends.GraphQLClient,
    uuid: LDAPUUID,
) -> None:
    logger.info("Received LDAP AMQP event (Reconcile)", uuid=uuid)

    if uuid in settings.ldap_uuids_to_ignore:
        logger.warning("LDAP event ignored due to ignore-list", ldap_uuid=uuid)
        return

    dn = await dataloader.ldapapi.get_ldap_dn(uuid)
    if dn is None:
        logger.error("LDAP UUID could not be found", uuid=uuid)
        raise RejectMessage("LDAP UUID could not be found")

    person_uuid = await dataloader.find_mo_employee_uuid(dn)
    if person_uuid is None:
        return
    # We handle reconciliation by seeding events into the normal processing queue
    # TODO: This ignores `event_namespace` and refreshes all integrations
    me = await graphql_client.who_am_i()
    await graphql_client.person_refresh(uuids=[person_uuid], owner=me.actor.uuid)


def configure_ldap_amqpsystem(fastramqpi: FastRAMQPI, settings: Settings) -> AMQPSystem:
    logger.info("Initializing LDAP AMQP system")
    ldap_amqpsystem = AMQPSystem(
        settings=settings.ldap_amqp,
        router=Router(),
        dependencies=[
            Depends(rate_limit(10)),
            Depends(logger_bound_message_id),
            Depends(request_id),
        ],
    )
    fastramqpi.add_context(ldap_amqpsystem=ldap_amqpsystem)
    ldap_amqpsystem.context = fastramqpi._context
    return ldap_amqpsystem
