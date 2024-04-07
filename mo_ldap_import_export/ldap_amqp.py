# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
from typing import Annotated

from fastapi import Depends
from fastramqpi.main import FastRAMQPI
from fastramqpi.ramqp import AMQPSystem
from fastramqpi.ramqp.amqp import Router
from fastramqpi.ramqp.depends import get_payload_as_type
from fastramqpi.ramqp.depends import rate_limit

from .config import LDAPAMQPConnectionSettings
from .depends import SyncTool
from .logging import logger


ldap_amqp_router = Router()

# Try errors again after a short period of time
delay_on_error = 10
RateLimit = Annotated[None, Depends(rate_limit(delay_on_error))]

PayloadDN = Annotated[str, Depends(get_payload_as_type(str))]


@ldap_amqp_router.register("dn")
async def process_dn(
    sync_tool: SyncTool,
    dn: PayloadDN,
    _: RateLimit,
) -> None:
    logger.bind(dn=dn)
    logger.info("Received LDAP AMQP event")
    await sync_tool.import_single_user(dn)


def configure_ldap_amqpsystem(
    fastramqpi: FastRAMQPI, settings: LDAPAMQPConnectionSettings
) -> None:
    logger.info("Initializing LDAP AMQP system")
    ldap_amqpsystem = AMQPSystem(settings=settings, router=ldap_amqp_router)
    fastramqpi.add_context(ldap_amqpsystem=ldap_amqpsystem)
    # 3100 because SyncTool, which is used by the above handler, is 3000
    fastramqpi.add_lifespan_manager(ldap_amqpsystem, 3100)
    ldap_amqpsystem.router.registry.update(ldap_amqp_router.registry)
    ldap_amqpsystem.context = fastramqpi._context
