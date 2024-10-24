# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Dataloaders to bulk requests."""

import asyncio
from contextlib import suppress
from datetime import datetime
from enum import Enum
from enum import auto
from typing import Any
from typing import cast
from uuid import UUID

import structlog
from fastramqpi.context import Context
from fastramqpi.ramqp.mo import MOAMQPSystem
from ldap3 import Connection
from more_itertools import bucket
from more_itertools import one
from more_itertools import partition

from mo_ldap_import_export.models import Address
from mo_ldap_import_export.models import Employee
from mo_ldap_import_export.models import Engagement
from mo_ldap_import_export.models import ITUser
from mo_ldap_import_export.models import MOBase
from mo_ldap_import_export.models import OrganisationUnit

from .autogenerated_graphql_client import AddressCreateInput
from .autogenerated_graphql_client import AddressUpdateInput
from .autogenerated_graphql_client import EmployeeCreateInput
from .autogenerated_graphql_client import EngagementCreateInput
from .autogenerated_graphql_client import EngagementUpdateInput
from .autogenerated_graphql_client import GraphQLClient
from .autogenerated_graphql_client import ITUserCreateInput
from .autogenerated_graphql_client import ITUserUpdateInput
from .autogenerated_graphql_client import OrganisationUnitCreateInput
from .autogenerated_graphql_client import RAValidityInput
from .autogenerated_graphql_client.input_types import AddressTerminateInput
from .autogenerated_graphql_client.input_types import ClassCreateInput
from .autogenerated_graphql_client.input_types import EngagementTerminateInput
from .autogenerated_graphql_client.input_types import ITUserTerminateInput
from .autogenerated_graphql_client.input_types import RAOpenValidityInput
from .config import Settings
from .exceptions import DNNotFound
from .exceptions import MultipleObjectsReturnedException
from .exceptions import NoObjectsReturnedException
from .ldap import is_uuid
from .ldapapi import LDAPAPI
from .moapi import MOAPI
from .types import DN
from .types import CPRNumber
from .utils import is_exception
from .utils import star

logger = structlog.stdlib.get_logger()


class Verb(Enum):
    CREATE = auto()
    EDIT = auto()
    TERMINATE = auto()


class DataLoader:
    def __init__(self, context: Context, amqpsystem: MOAMQPSystem) -> None:
        self.context = context
        self.user_context = context["user_context"]
        self.ldap_connection: Connection = self.user_context["ldap_connection"]
        self.settings: Settings = self.user_context["settings"]
        self.ldapapi = LDAPAPI(self.settings, self.ldap_connection)
        self.create_mo_class_lock = asyncio.Lock()
        self.amqpsystem: MOAMQPSystem = amqpsystem

    @property
    def moapi(self) -> MOAPI:
        return MOAPI(self.settings, self.graphql_client)

    @property
    def graphql_client(self) -> GraphQLClient:
        return cast(GraphQLClient, self.context["graphql_client"])

    @property
    def sync_tool(self):
        from .import_export import SyncTool

        return cast(SyncTool, self.user_context["sync_tool"])

    @property
    def converter(self):
        from .converters import LdapConverter

        return cast(LdapConverter, self.user_context["converter"])

    @property
    def username_generator(self):
        from .usernames import UserNameGenerator

        return cast(UserNameGenerator, self.user_context["username_generator"])

    async def find_mo_employee_uuid_via_cpr_number(self, dn: str) -> set[UUID]:
        cpr_number = await self.ldapapi.dn2cpr(dn)
        if cpr_number is None:
            return set()

        result = await self.graphql_client.read_employee_uuid_by_cpr_number(cpr_number)
        return {employee.uuid for employee in result.objects}

    async def find_mo_employee_uuid(self, dn: str) -> UUID | None:
        cpr_results = await self.find_mo_employee_uuid_via_cpr_number(dn)
        if len(cpr_results) == 1:
            uuid = one(cpr_results)
            logger.info("Found employee via CPR matching", dn=dn, uuid=uuid)
            return uuid

        unique_uuid = await self.ldapapi.get_ldap_unique_ldap_uuid(dn)
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

    def extract_unique_ldap_uuids(self, it_users: list[ITUser]) -> set[UUID]:
        """
        Extracts unique ldap uuids from a list of it-users
        """
        it_user_keys = {ituser.user_key for ituser in it_users}
        not_uuids, uuids = partition(is_uuid, it_user_keys)
        not_uuid_set = set(not_uuids)
        if not_uuid_set:
            logger.warning("Non UUID IT-user user-keys", user_keys=not_uuid_set)
            raise ExceptionGroup(
                "Exceptions during IT-user UUID extraction",
                [
                    ValueError(f"Non UUID IT-user user-key: {user_key}")
                    for user_key in not_uuid_set
                ],
            )
        # TODO: Check for duplicates?
        return set(map(UUID, uuids))

    async def find_mo_employee_dn_by_itsystem(self, uuid: UUID) -> set[DN]:
        """Tries to find the LDAP DNs belonging to a MO employee via ITUsers.

        Args:
            uuid: UUID of the employee to try to find DNs for.

        Returns:
            A potentially empty set of DNs.
        """
        # TODO: How do we know if the ITUser is up-to-date with the newest DNs in AD?

        # The ITSystem only exists if configured to do so
        raw_it_system_uuid = await self.moapi.get_ldap_it_system_uuid()
        # If it does not exist, we cannot fetch users for it
        if raw_it_system_uuid is None:
            return set()

        it_system_uuid = UUID(raw_it_system_uuid)
        it_users = await self.moapi.load_mo_employee_it_users(uuid, it_system_uuid)
        ldap_uuids = self.extract_unique_ldap_uuids(it_users)
        dns = await self.ldapapi.convert_ldap_uuids_to_dns(ldap_uuids)
        # No DNs, no problem
        if not dns:
            return set()

        # If we have one or more ITUsers (with valid dns), return those
        logger.info(
            "Found DN(s) using ITUser lookup",
            dns=dns,
            employee_uuid=uuid,
        )
        return dns

    async def find_mo_employee_dn_by_cpr_number(self, uuid: UUID) -> set[DN]:
        """Tries to find the LDAP DNs belonging to a MO employee via CPR numbers.

        Args:
            uuid: UUID of the employee to try to find DNs for.

        Returns:
            A potentially empty set of DNs.
        """
        # If the employee has a cpr-no, try using that to find matchind DNs
        employee = await self.moapi.load_mo_employee(uuid)
        if employee is None:
            raise NoObjectsReturnedException(f"Unable to lookup employee: {uuid}")
        cpr_number = CPRNumber(employee.cpr_number) if employee.cpr_number else None
        # No CPR, no problem
        if not cpr_number:
            return set()

        logger.info(
            "Attempting CPR number lookup",
            employee_uuid=uuid,
        )
        dns = set()
        with suppress(NoObjectsReturnedException):
            dns = await self.ldapapi.cpr2dns(cpr_number)
        if not dns:
            return set()
        logger.info(
            "Found DN(s) using CPR number lookup",
            dns=dns,
            employee_uuid=uuid,
        )
        return dns

    async def find_mo_employee_dn(self, uuid: UUID) -> set[DN]:
        """Tries to find the LDAP DNs belonging to a MO employee.

        Args:
            uuid: UUID of the employee to try to find DNs for.

        Returns:
            A potentially empty set of DNs.
        """
        # TODO: This should probably return a list of EntityUUIDs rather than DNs
        #       However this should probably be a change away from DNs in general
        logger.info(
            "Attempting to find DNs",
            employee_uuid=uuid,
        )
        # TODO: We should be able to trust just the ITUsers, however we do not.
        #       Maybe once the code becomes easier to reason about, we can get to that.
        #       But for now, we fetch all accounts, and use the discriminator.
        #
        # TODO: We may want to expand this in the future to also check for half-created
        #       objects, to support scenarios where the application may crash after
        #       creating an LDAP account, but before making a MO ITUser.
        ituser_dns, cpr_number_dns = await asyncio.gather(
            self.find_mo_employee_dn_by_itsystem(uuid),
            self.find_mo_employee_dn_by_cpr_number(uuid),
        )
        dns = ituser_dns | cpr_number_dns
        if dns:
            return dns
        logger.warning(
            "Unable to find DNs for MO employee",
            employee_uuid=uuid,
        )
        return set()

    # TODO: move to synctool
    async def make_mo_employee_dn(self, uuid: UUID) -> DN:
        employee = await self.moapi.load_mo_employee(uuid)
        if employee is None:
            raise NoObjectsReturnedException(f"Unable to lookup employee: {uuid}")
        cpr_number = CPRNumber(employee.cpr_number) if employee.cpr_number else None

        # Check if we even dare create a DN
        raw_it_system_uuid = await self.moapi.get_ldap_it_system_uuid()
        if raw_it_system_uuid is None and cpr_number is None:
            logger.warning(
                "Could not or generate a DN for employee (cannot correlate)",
                employee_uuid=uuid,
            )
            raise DNNotFound("Unable to generate DN, no correlation key available")

        # If we did not find a DN neither via ITUser nor via CPR-number, then we want
        # to create one, by generating a DN, importing the user and potentially creating
        # a binding between the two.

        logger.info("Generating DN for user", employee_uuid=uuid)
        # NOTE: This not only generates the DN as the name suggests,
        #       rather it also *creates it in LDAP*, be warned!
        #
        #       Additionally it turns out that it does not only create the DN in LDAP
        #       rather it uploads the entire employee object to LDAP.
        #
        # TODO: Does this upload actively require a cpr_number on the employee?
        #       If we do not have the CPR number nor the ITSystem, we would be leaking
        #       the DN we generate, so maybe we should guard for this, the old code seemed
        #       to do so, maybe we should simply not upload anything in that case.
        dn = await self.username_generator.generate_dn(employee)
        assert isinstance(dn, str)

        # If the LDAP ITSystem exists, we want to create a binding to our newly
        # generated (and created) DN, such that it can be correlated in the future.
        #
        # NOTE: This may not be executed if the program crashes after the above line,
        #       thus the current code is not robust and may fail at any time.
        #       The appropriate solution here is to ensure that generate_dn atomically
        #       creates a link between the MO entity and the newly created LDAP entity,
        #       such as by adding the MO UUID to the newly created LDAP entity.
        if raw_it_system_uuid is not None:
            logger.info(
                "No ITUser found, creating one to correlate with DN",
                employee_uuid=uuid,
                dn=dn,
            )
            # Get its unique ldap uuid
            # TODO: Get rid of this code and operate on EntityUUIDs thoughout
            unique_uuid = await self.ldapapi.get_ldap_unique_ldap_uuid(dn)
            logger.info(
                "LDAP UUID found for DN",
                employee_uuid=uuid,
                dn=dn,
                ldap_uuid=unique_uuid,
            )
            # Make a new it-user
            it_user = ITUser.from_simplified_fields(
                str(unique_uuid),
                UUID(raw_it_system_uuid),
                datetime.today().strftime("%Y-%m-%d"),
                person_uuid=uuid,
            )
            # from_simplified_fields() has bad type annotation
            assert isinstance(it_user, ITUser)
            await self.create_ituser(it_user)

        return dn

    async def create_or_edit_mo_objects(
        self, objects: list[tuple[MOBase, Verb]]
    ) -> None:
        # TODO: the TERMINATE verb should definitely be emitted directly in
        # format_converted_objects instead.
        def fix_verb(obj: MOBase, verb: Verb) -> tuple[MOBase, Verb]:
            if hasattr(obj, "terminate_"):
                return obj, Verb.TERMINATE
            return obj, verb

        # HACK to set termination verb, should be set within format_converted_objects instead,
        # but doing so requires restructuring the entire flow of the integration, which is a major
        # task best saved for later.
        objects = [fix_verb(obj, verb) for obj, verb in objects]

        # Split objects into groups
        verb_groups = bucket(objects, key=star(lambda _, verb: verb))
        creates = verb_groups[Verb.CREATE]
        edits = verb_groups[Verb.EDIT]
        terminates = verb_groups[Verb.TERMINATE]

        await asyncio.gather(
            self.create([obj for obj, _ in creates]),
            self.edit([obj for obj, _ in edits]),
            self.terminate([obj for obj, _ in terminates]),
        )

    async def create_employee(self, obj: Employee) -> None:
        await self.graphql_client.user_create(
            input=EmployeeCreateInput(
                uuid=obj.uuid,
                user_key=obj.user_key,
                given_name=obj.given_name,
                surname=obj.surname,
                seniority=obj.seniority,
                cpr_number=obj.cpr_number,
                nickname_given_name=obj.nickname_given_name,
                nickname_surname=obj.nickname_surname,
            ),
        )

    async def create_address(self, obj: Address) -> None:
        assert obj.person is not None
        assert obj.org_unit is None
        assert obj.value2 is None
        await self.graphql_client.address_create(
            input=AddressCreateInput(
                uuid=obj.uuid,
                user_key=obj.user_key,
                value=obj.value,
                address_type=obj.address_type,
                person=obj.person,
                engagement=obj.engagement,
                visibility=obj.visibility,
                validity=RAValidityInput(
                    from_=obj.validity.start,
                    to=obj.validity.end,
                ),
            ),
        )

    async def create_engagement(self, obj: Engagement) -> None:
        await self.graphql_client.engagement_create(
            input=EngagementCreateInput(
                uuid=obj.uuid,
                user_key=obj.user_key,
                org_unit=obj.org_unit.uuid,
                person=obj.person.uuid,
                job_function=obj.job_function.uuid,
                engagement_type=obj.engagement_type.uuid,
                primary=obj.primary.uuid if obj.primary is not None else None,
                extension_1=obj.extension_1,
                extension_2=obj.extension_2,
                extension_3=obj.extension_3,
                extension_4=obj.extension_4,
                extension_5=obj.extension_5,
                extension_6=obj.extension_6,
                extension_7=obj.extension_7,
                extension_8=obj.extension_8,
                extension_9=obj.extension_9,
                extension_10=obj.extension_10,
                validity=RAValidityInput(
                    from_=obj.validity.from_date,
                    to=obj.validity.to_date,
                ),
            )
        )

    async def create_ituser(self, obj: ITUser) -> None:
        await self.graphql_client.ituser_create(
            input=ITUserCreateInput(
                uuid=obj.uuid,
                user_key=obj.user_key,
                itsystem=obj.itsystem.uuid,
                person=obj.person.uuid if obj.person is not None else None,
                org_unit=obj.org_unit.uuid if obj.org_unit is not None else None,
                engagement=obj.engagement.uuid if obj.engagement is not None else None,
                validity=RAValidityInput(
                    from_=obj.validity.from_date,
                    to=obj.validity.to_date,
                ),
            )
        )

    async def create_org_unit(self, obj: OrganisationUnit) -> None:
        assert obj.details is None
        await self.graphql_client.org_unit_create(
            input=OrganisationUnitCreateInput(
                uuid=obj.uuid,
                user_key=obj.user_key,
                name=obj.name,
                parent=obj.parent.uuid if obj.parent is not None else None,
                org_unit_hierarchy=(
                    obj.org_unit_hierarchy.uuid
                    if obj.org_unit_hierarchy is not None
                    else None
                ),
                org_unit_type=obj.org_unit_type.uuid,
                org_unit_level=obj.org_unit_level.uuid,
                validity=RAValidityInput(
                    from_=obj.validity.from_date,
                    to=obj.validity.to_date,
                ),
            ),
        )

    async def create_object(self, obj: MOBase) -> None:
        if isinstance(obj, Address):
            await self.create_address(obj)
        elif isinstance(obj, Employee):
            await self.create_employee(obj)
        elif isinstance(obj, Engagement):
            await self.create_engagement(obj)
        elif isinstance(obj, ITUser):
            await self.create_ituser(obj)
        else:  # pragma: no cover
            raise NotImplementedError(f"Unable to create {obj}")

    async def create(self, creates: list[MOBase]) -> None:
        tasks = [self.create_object(obj) for obj in creates]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        exceptions = cast(list[Exception], list(filter(is_exception, results)))
        if exceptions:  # pragma: no cover
            raise ExceptionGroup("Exceptions during creation", exceptions)

    async def edit_employee(self, obj: Employee) -> None:  # pragma: no cover
        # TODO: see comment in import_export.py:format_converted_objects()
        raise NotImplementedError("cannot edit employee using ramodels object")

    async def edit_address(self, obj: Address) -> None:
        assert obj.person is not None
        assert obj.org_unit is None
        assert obj.value2 is None
        await self.graphql_client.address_update(
            input=AddressUpdateInput(
                uuid=obj.uuid,
                user_key=obj.user_key,
                value=obj.value,
                address_type=obj.address_type,
                person=obj.person,
                engagement=obj.engagement,
                visibility=obj.visibility,
                validity=RAValidityInput(
                    from_=obj.validity.start,
                    to=obj.validity.end,
                ),
            ),
        )

    async def edit_engagement(self, obj: Engagement) -> None:
        await self.graphql_client.engagement_update(
            input=EngagementUpdateInput(
                uuid=obj.uuid,
                user_key=obj.user_key,
                org_unit=obj.org_unit.uuid,
                person=obj.person.uuid,
                job_function=obj.job_function.uuid,
                engagement_type=obj.engagement_type.uuid,
                primary=obj.primary.uuid if obj.primary is not None else None,
                extension_1=obj.extension_1,
                extension_2=obj.extension_2,
                extension_3=obj.extension_3,
                extension_4=obj.extension_4,
                extension_5=obj.extension_5,
                extension_6=obj.extension_6,
                extension_7=obj.extension_7,
                extension_8=obj.extension_8,
                extension_9=obj.extension_9,
                extension_10=obj.extension_10,
                validity=RAValidityInput(
                    from_=obj.validity.from_date,
                    to=obj.validity.to_date,
                ),
            )
        )

    async def edit_ituser(self, obj: ITUser) -> None:
        await self.graphql_client.ituser_update(
            input=ITUserUpdateInput(
                uuid=obj.uuid,
                user_key=obj.user_key,
                itsystem=obj.itsystem.uuid,
                person=obj.person.uuid if obj.person is not None else None,
                org_unit=obj.org_unit.uuid if obj.org_unit is not None else None,
                engagement=obj.engagement.uuid if obj.engagement is not None else None,
                validity=RAValidityInput(
                    from_=obj.validity.from_date,
                    to=obj.validity.to_date,
                ),
            )
        )

    async def edit_object(self, obj: MOBase) -> None:
        if isinstance(obj, Address):
            await self.edit_address(obj)
        elif isinstance(obj, Employee):  # pragma: no cover
            await self.edit_employee(obj)
        elif isinstance(obj, Engagement):
            await self.edit_engagement(obj)
        elif isinstance(obj, ITUser):
            await self.edit_ituser(obj)
        else:  # pragma: no cover
            raise NotImplementedError(f"Unable to edit {obj}")

    async def edit(self, edits: list[MOBase]) -> None:
        tasks = [self.edit_object(obj) for obj in edits]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        exceptions = cast(list[Exception], list(filter(is_exception, results)))
        if exceptions:  # pragma: no cover
            raise ExceptionGroup("Exceptions during modification", exceptions)

    async def terminate_address(self, uuid: UUID, at: datetime) -> None:
        await self.graphql_client.address_terminate(
            AddressTerminateInput(uuid=uuid, to=at)
        )

    async def terminate_engagement(self, uuid: UUID, at: datetime) -> None:
        await self.graphql_client.engagement_terminate(
            EngagementTerminateInput(uuid=uuid, to=at)
        )

    async def terminate_ituser(self, uuid: UUID, at: datetime) -> None:
        await self.graphql_client.ituser_terminate(
            ITUserTerminateInput(uuid=uuid, to=at)
        )

    async def terminate_object(self, uuid: UUID, at: datetime, motype: type) -> None:
        """Terminate a detail."""
        if issubclass(motype, Address):
            await self.terminate_address(uuid, at)
        elif issubclass(motype, Engagement):
            await self.terminate_engagement(uuid, at)
        elif issubclass(motype, ITUser):
            await self.terminate_ituser(uuid, at)
        else:  # pragma: no cover
            raise NotImplementedError(f"Unable to terminate {motype}")

    async def terminate(self, terminatees: list[Any]) -> None:
        """Terminate a list of details.

        This method calls `terminate_object` for each objects in parallel.

        Args:
            terminatees: The list of details to terminate.

        Returns:
            UUIDs of the terminated entries
        """
        detail_terminations: list[dict[str, Any]] = [
            {
                "motype": type(terminate),
                "uuid": terminate.uuid,
                "at": terminate.terminate_,
            }
            for terminate in terminatees
        ]
        tasks = [self.terminate_object(**detail) for detail in detail_terminations]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        exceptions = cast(list[Exception], list(filter(is_exception, results)))
        if exceptions:  # pragma: no cover
            raise ExceptionGroup("Exceptions during termination", exceptions)

    async def create_mo_class(
        self,
        name: str,
        user_key: str,
        facet_uuid: UUID,
        scope: str | None = None,
    ) -> UUID:
        """Creates a class in MO.

        Args:
            name: The name for the class.
            user_key: The user-key for the class.
            facet_uuid: The UUID of the facet to attach this class to.
            scope: The optional scope to assign to the class.

        Returns:
            The uuid of the existing or newly created class.
        """
        async with self.create_mo_class_lock:
            # If class already exists, noop
            uuid = await self.moapi.load_mo_class_uuid(user_key)
            if uuid:
                logger.info("MO class exists", user_key=user_key)
                return uuid

            logger.info("Creating MO class", user_key=user_key)
            input = ClassCreateInput(
                name=name,
                user_key=user_key,
                facet_uuid=facet_uuid,
                scope=scope,
                validity=RAOpenValidityInput(from_=None),
            )
            result = await self.graphql_client.class_create(input)
            return result.uuid
