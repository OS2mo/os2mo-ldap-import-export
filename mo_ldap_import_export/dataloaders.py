# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Dataloaders to bulk requests."""

import asyncio
from typing import Any
from uuid import UUID

import structlog
from ldap3.core.exceptions import LDAPNoSuchObjectResult
from more_itertools import duplicates_everseen
from more_itertools import one

from .config import Settings
from .exceptions import MultipleObjectsReturnedException
from .exceptions import NoObjectsReturnedException
from .exceptions import RequeueException
from .ldap import apply_discriminator
from .ldap import filter_dns
from .ldap import is_uuid
from .ldap import make_ldap_object
from .ldap import object_search
from .ldap_classes import LdapObject
from .ldapapi import LDAPAPI
from .moapi import MOAPI
from .models import ITUser
from .types import DN
from .types import LDAPUUID
from .types import CPRNumber
from .types import EmployeeUUID
from .utils import combine_dn_strings
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

    async def find_mo_employee_dn_by_itsystem(self, uuid: EmployeeUUID) -> set[DN]:
        """Tries to find the LDAP DNs belonging to a MO employee using ITUser lookup."""
        raw_it_system_uuid = await self.moapi.get_ldap_it_system_uuid()
        if raw_it_system_uuid is None:
            return set()
        it_system_uuid = UUID(raw_it_system_uuid)
        it_users = await self.moapi.load_mo_employee_it_users(uuid, it_system_uuid)
        ldap_uuid_ituser_map = extract_unique_ldap_uuids(it_users)
        ldap_uuids = set(ldap_uuid_ituser_map.keys())
        if not ldap_uuids:
            return set()

        # Batch fetch for performance
        dns_map = await self.ldapapi.convert_ldap_uuids_to_dns(ldap_uuids)

        # Cleanup IT-users that point to non-existing LDAP accounts
        missing_dn_uuids = {u for u, dn in dns_map.items() if not dn}
        if missing_dn_uuids:
            async with asyncio.TaskGroup() as tg:
                for luuid in missing_dn_uuids:
                    ituser = ldap_uuid_ituser_map[luuid]
                    logger.info("Terminating correlation link it-user", uuid=ituser.uuid)
                    tg.create_task(self.moapi.terminate_ituser(ituser.uuid, mo_today()))

        # Log results for existing tests
        found_dns = {dn for dn in dns_map.values() if dn}
        if found_dns:
            logger.info(
                "Found DN(s) using ITUser lookup", dns=found_dns, employee_uuid=uuid
            )

        return found_dns

    async def find_mo_employee_ldap_objects(self, uuid: UUID) -> dict[DN, LdapObject]:
        # 1. Get Search Criteria
        raw_it_system_uuid = await self.moapi.get_ldap_it_system_uuid()
        it_system_uuid = UUID(raw_it_system_uuid) if raw_it_system_uuid else None

        it_users = []
        if it_system_uuid:
            it_users = await self.moapi.load_mo_employee_it_users(uuid, it_system_uuid)

        ldap_uuid_ituser_map = extract_unique_ldap_uuids(it_users)
        ldap_uuids = set(ldap_uuid_ituser_map.keys())

        employee = await self.moapi.load_mo_employee(uuid)
        cpr_number = (
            CPRNumber(employee.cpr_number) if employee and employee.cpr_number else None
        )

        if not ldap_uuids and not cpr_number:
            return {}

        # 2. Build Filter
        filters = []
        unique_id_field = self.settings.ldap_unique_id_field
        for ldap_uuid in ldap_uuids:
            filters.append(f"({unique_id_field}={ldap_uuid})")

        if cpr_number and self.settings.ldap_cpr_attribute:
            filters.append(f"({self.settings.ldap_cpr_attribute}={cpr_number})")

        combined_filter = f"(&(objectclass=*)(|{''.join(filters)}))"

        # 3. Search
        attributes = {"*", unique_id_field}

        search_base = self.settings.ldap_search_base
        ous_to_search_in = self.settings.ldap_ous_to_search_in
        search_bases = [
            combine_dn_strings([ou, search_base]) for ou in ous_to_search_in
        ]

        searchParameters = {
            "search_base": search_bases,
            "search_filter": combined_filter,
            "attributes": list(attributes),
        }

        try:
            search_results = await object_search(
                searchParameters, self.ldapapi.connection
            )
            found_objects = [
                await make_ldap_object(res, self.ldapapi.connection, nest=False)
                for res in search_results
            ]
        except LDAPNoSuchObjectResult:
            found_objects = []

        # 4. Map results and Cleanup
        # Map objects by DN
        dn_object_map = {obj.dn: obj for obj in found_objects}

        # Map objects by UUID (for ITUser cleanup)
        uuid_object_map = {}
        for obj in found_objects:
            val = getattr(obj, unique_id_field, None)
            if val:
                uuid_object_map[LDAPUUID(str(val))] = obj

        # ITUser Cleanup
        missing_dn_uuids = ldap_uuids - set(uuid_object_map.keys())
        missing_dn_mo_uuid = {
            ldap_uuid_ituser_map[ldap_uuid].uuid for ldap_uuid in missing_dn_uuids
        }

        if missing_dn_mo_uuid:
            async with asyncio.TaskGroup() as tg:
                for mo_uuid in missing_dn_mo_uuid:
                    logger.info("Terminating correlation link it-user", uuid=mo_uuid)
                    tg.create_task(self.moapi.terminate_ituser(mo_uuid, mo_today()))

        if dn_object_map:
            logger.info("Found objects", dns=set(dn_object_map.keys()))

        return dn_object_map

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
