# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Dataloaders to bulk requests."""

import asyncio
from contextlib import suppress
from typing import Any
from typing import cast
from uuid import UUID

import structlog
from more_itertools import duplicates_everseen
from more_itertools import one

from .config import Settings
from .exceptions import MultipleObjectsReturnedException
from .exceptions import NoObjectsReturnedException
from .exceptions import RequeueException
from .ldap import apply_discriminator
from .ldap import filter_dns
from .ldap import is_uuid
from .ldap_classes import LdapObject
from .ldapapi import LDAPAPI
from .moapi import MOAPI
from .models import ITUser
from .types import DN
from .types import LDAPUUID
from .types import CPRNumber
from .types import EmployeeUUID
from .utils import mo_today

logger = structlog.stdlib.get_logger()


class NoGoodLDAPAccountFound(ValueError):
    pass


def extract_unique_ldap_uuids(it_users: list[ITUser]) -> dict[LDAPUUID, ITUser]:
    """
    Extracts unique ldap uuids from a list of it-users
    """
    it_user_keys = [ituser.user_key for ituser in it_users]
    not_uuid_set = {user_key for user_key in it_user_keys if not is_uuid(user_key)}
    if not_uuid_set:
        logger.error("Non UUID IT-user user-keys", user_keys=not_uuid_set)
        raise ExceptionGroup(
            "Exceptions during IT-user UUID extraction",
            [
                ValueError(f"Non UUID IT-user user-key: {user_key}")
                for user_key in not_uuid_set
            ],
        )

    duplicates = set(duplicates_everseen(it_user_keys))
    if duplicates:
        logger.error("Duplicate UUID IT-user", user_keys=duplicates)
        raise ExceptionGroup(
            "Duplicates during IT-user UUID extraction",
            [
                ValueError(f"Duplicate UUID IT-user user-key: {user_key}")
                for user_key in duplicates
            ],
        )

    return {LDAPUUID(ituser.user_key): ituser for ituser in it_users}


class DataLoader:
    def __init__(
        self, settings: Settings, moapi: MOAPI, ldapapi: LDAPAPI, username_generator
    ) -> None:
        self.settings = settings
        self.ldapapi = ldapapi
        self.moapi = moapi
        self.username_generator = username_generator

    async def find_mo_employee_uuid_via_cpr_number(
        self, dn: DN, ldap_object: Any | None = None
    ) -> set[EmployeeUUID]:
        cpr_number = await self.ldapapi.dn2cpr(dn, ldap_object=ldap_object)
        if cpr_number is None:
            return set()
        return await self.moapi.cpr2uuids(cpr_number)

    async def find_mo_employee_uuid(
        self, dn: DN, ldap_object: Any | None = None
    ) -> EmployeeUUID | None:
        cpr_results = await self.find_mo_employee_uuid_via_cpr_number(
            dn, ldap_object=ldap_object
        )
        if len(cpr_results) == 1:
            uuid = one(cpr_results)
            logger.info("Found employee via CPR matching", dn=dn, uuid=uuid)
            return uuid

        unique_uuid = await self.ldapapi.get_ldap_unique_ldap_uuid(
            dn, ldap_object=ldap_object
        )
        ituser_results = await self.moapi.find_mo_employee_uuid_via_ituser(unique_uuid)
        if len(ituser_results) == 1:
            uuid = one(ituser_results)
            logger.info("Found employee via ITUser matching", dn=dn, uuid=uuid)
            return uuid

        # TODO: Return an ExceptionGroup with both
        # NOTE: This may break a lot of things, because we explicitly match against MultipleObjectsReturnedException
        if len(cpr_results) > 1:
            raise MultipleObjectsReturnedException(f"Multiple CPR matches for dn={dn}")

        if len(ituser_results) > 1:
            raise MultipleObjectsReturnedException(
                f"Multiple ITUser matches for dn={dn}"
            )

        logger.info("No matching employee", dn=dn)
        return None

    async def find_mo_employee_ldap_objects_by_itsystem(
        self, uuid: UUID
    ) -> dict[DN, LdapObject]:
        """Tries to find the LDAP Objects belonging to a MO employee via ITUsers."""
        raw_it_system_uuid = await self.moapi.get_ldap_it_system_uuid()
        if raw_it_system_uuid is None:
            return {}

        it_system_uuid = UUID(raw_it_system_uuid)
        it_users = await self.moapi.load_mo_employee_it_users(uuid, it_system_uuid)
        ldap_uuid_ituser_map = extract_unique_ldap_uuids(it_users)
        ldap_uuids = set(ldap_uuid_ituser_map.keys())
        
        uuid_object_map = await self.ldapapi.convert_ldap_uuids_to_objects(ldap_uuids)

        # Find the LDAP UUIDs that could not be mapped to objects
        missing_dn_uuids = {
            ldap_uuid for ldap_uuid, obj in uuid_object_map.items() if obj is None
        }
        missing_dn_mo_uuid = {
            ldap_uuid_ituser_map[ldap_uuid].uuid for ldap_uuid in missing_dn_uuids
        }
        
        async with asyncio.TaskGroup() as tg:
            for mo_uuid in missing_dn_mo_uuid:
                logger.info("Terminating correlation link it-user", uuid=mo_uuid)
                tg.create_task(self.moapi.terminate_ituser(mo_uuid, mo_today()))

        objects = {
            obj.dn: obj for obj in uuid_object_map.values() if obj is not None
        }
        if not objects:
            return {}

        logger.info(
            "Found objects using ITUser lookup",
            dns=set(objects.keys()),
            employee_uuid=uuid,
        )
        return objects

    async def find_mo_employee_dn_by_itsystem(self, uuid: UUID) -> set[DN]:
        objects = await self.find_mo_employee_ldap_objects_by_itsystem(uuid)
        return set(objects.keys())

    async def find_mo_employee_ldap_objects_by_cpr_number(
        self, uuid: UUID
    ) -> dict[DN, LdapObject]:
        """Tries to find the LDAP Objects belonging to a MO employee via CPR numbers."""
        employee = await self.moapi.load_mo_employee(uuid)
        if employee is None:
            raise NoObjectsReturnedException(f"Unable to lookup employee: {uuid}")
        cpr_number = CPRNumber(employee.cpr_number) if employee.cpr_number else None
        if not cpr_number:
            return {}

        logger.info("Attempting CPR number lookup", employee_uuid=uuid)
        ldap_objects = await self.ldapapi.cpr2objects(cpr_number)
        
        objects = {obj.dn: obj for obj in ldap_objects}
        if not objects:
            return {}
            
        logger.info(
            "Found objects using CPR number lookup",
            dns=set(objects.keys()),
            employee_uuid=uuid,
        )
        return objects

    async def find_mo_employee_dn_by_cpr_number(self, uuid: UUID) -> set[DN]:
        objects = await self.find_mo_employee_ldap_objects_by_cpr_number(uuid)
        return set(objects.keys())

    async def find_mo_employee_ldap_objects(self, uuid: UUID) -> dict[DN, LdapObject]:
        ituser_objects, cpr_objects = await asyncio.gather(
            self.find_mo_employee_ldap_objects_by_itsystem(uuid),
            self.find_mo_employee_ldap_objects_by_cpr_number(uuid),
        )
        combined = ituser_objects.copy()
        combined.update(cpr_objects)
        
        if combined:
            logger.info("Found DNs/Objects for MO employee", employee_uuid=uuid, dns=set(combined.keys()))
            return combined
            
        logger.warning("Unable to find DNs for MO employee", employee_uuid=uuid)
        return {}

    async def find_mo_employee_dn(self, uuid: UUID) -> set[DN]:
        """Tries to find the LDAP DNs belonging to a MO employee.

        Args:
            uuid: UUID of the employee to try to find DNs for.

        Raises:
            NoObjectsReturnedException: If the MO employee could not be found.

        Returns:
            A potentially empty set of DNs.
        """
        objects = await self.find_mo_employee_ldap_objects(uuid)
        return set(objects.keys())

    async def make_mo_employee_dn(
        self, uuid: UUID, common_name: str | None = None
    ) -> DN:
        employee = await self.moapi.load_mo_employee(uuid)
        if employee is None:
            raise NoObjectsReturnedException(f"Unable to lookup employee: {uuid}")
        cpr_number = CPRNumber(employee.cpr_number) if employee.cpr_number else None

        # Check if we even dare create a DN, we need a correlation key before we dare
        if cpr_number is None:
            raw_it_system_uuid = await self.moapi.get_ldap_it_system_uuid()
            if raw_it_system_uuid is None:
                logger.warning(
                    "Refused to generate a DN for employee (no correlation key)",
                    employee_uuid=uuid,
                )
                raise RequeueException(
                    "Unable to generate DN, no correlation key available"
                )

        logger.info("Generating DN for user", employee_uuid=uuid)
        if common_name is None:
            common_name = await self.username_generator.generate_common_name(employee)
        dn = await self.username_generator.generate_dn(common_name)
        assert isinstance(dn, str)
        return dn

    async def _find_best_ldap_object(
        self, uuid: EmployeeUUID, ldap_object: LdapObject | None = None
    ) -> LdapObject | None:
        """Find the best possible LDAP Object for the given user."""
        objects = await self.find_mo_employee_ldap_objects(uuid)
        
        dns = set(objects.keys())
        if ldap_object and ldap_object.dn in dns:
            pass
        elif ldap_object:
            dns.add(ldap_object.dn)
            objects[ldap_object.dn] = ldap_object

        dns = await filter_dns(
            self.settings,
            self.ldapapi.connection,
            dns,
            ldap_object=ldap_object,
            objects_cache=objects,
        )
        
        if not dns:
            return None
        logger.info("Found DNs for user", dns=dns, uuid=uuid)
        best_dn = await apply_discriminator(
            self.settings,
            self.ldapapi.connection,
            self.moapi,
            uuid,
            dns,
            ldap_object=ldap_object,
            objects_cache=objects,
        )
        if not best_dn:
            logger.warning(
                "Aborting synchronization, as no good LDAP account was found",
                dns=dns,
                uuid=uuid,
            )
            raise NoGoodLDAPAccountFound("Aborting synchronization")
        
        # Return the object corresponding to best_dn
        # Ideally we have it in `objects`.
        # If apply_discriminator returned a DN that was in `dns`, it should be in `objects`.
        # Unless apply_discriminator somehow synthesized a DN? Unlikely.
        # But `filter_dns` takes `dns` (set of strings).
        # We need to ensure we can lookup the object.
        
        if best_dn in objects:
            return objects[best_dn]
        
        # Fallback if not found (should not happen if logic is consistent)
        # Fetch it?
        return await self.ldapapi.get_object_by_dn(best_dn, {"*"})

    async def _find_best_dn(
        self, uuid: EmployeeUUID, ldap_object: LdapObject | None = None
    ) -> DN | None:
        """Find the best possible DN for the given user.

        Args:
            uuid: The MO UUID of the person to lookup.

        Raises:
            NoObjectsReturnedException: If the MO employee could not be found.
            NoGoodLDAPAccountFound: If no good LDAP account could be found.

        Returns:
            The best DN or None if no LDAP account was found.

        Note:
            Notice the distinction between the function returning None and raising
            NoGoodLDAPAccountFound. The former is a signal that an account can be
            created, while the latter is a signal that an account was found, and that
            synchronization should not take place.
        """
        obj = await self._find_best_ldap_object(uuid, ldap_object)
        return obj.dn if obj else None
