# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from uuid import UUID

import structlog

from .dataloaders import DataLoader
from .exceptions import IgnoreChanges

logger = structlog.stdlib.get_logger()


class ExportChecks:
    """
    Class with modules that are invoked when exporting data to LDAP
    """

    def __init__(self, dataloader: DataLoader) -> None:
        self.dataloader = dataloader

    async def check_it_user(self, employee_uuid: UUID, it_system_user_key: str):
        if not it_system_user_key:
            return

        it_system_uuid = await self.dataloader.moapi.get_it_system_uuid(
            it_system_user_key
        )
        it_users = await self.dataloader.moapi.load_mo_employee_it_users(
            employee_uuid, UUID(it_system_uuid)
        )

        if not it_users:
            raise IgnoreChanges(
                f"employee with uuid = {employee_uuid} "
                f"does not have an it-user with user_key = {it_system_user_key}"
            )
