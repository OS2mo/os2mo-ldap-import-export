# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Annotated

import structlog
from fastapi import APIRouter
from fastapi import Depends
from fastramqpi.events import Event
from fastramqpi.ramqp.depends import get_payload_as_type
from fastramqpi.ramqp.utils import RejectMessage

from . import depends
from .depends import DataLoader
from .depends import Settings
from .depends import SyncTool
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
    event: Event[LDAPUUID],
) -> None:
    uuid = event.subject
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
    event: Event[LDAPUUID],
) -> None:
    uuid = event.subject
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
