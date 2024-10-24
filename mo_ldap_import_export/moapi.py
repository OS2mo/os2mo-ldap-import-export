# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import asyncio
from collections.abc import Generator
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from typing import Protocol
from typing import TypeVar
from typing import cast
from uuid import UUID

import structlog
from fastapi.encoders import jsonable_encoder
from fastramqpi.ramqp.utils import RequeueMessage
from more_itertools import one
from more_itertools import only
from more_itertools import partition

from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    EmployeeFilter,
)
from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    EngagementFilter,
)
from mo_ldap_import_export.models import Address
from mo_ldap_import_export.models import Employee
from mo_ldap_import_export.models import Engagement
from mo_ldap_import_export.models import ITUser
from mo_ldap_import_export.types import EmployeeUUID
from mo_ldap_import_export.types import OrgUnitUUID
from mo_ldap_import_export.utils import star

from .autogenerated_graphql_client import GraphQLClient
from .autogenerated_graphql_client.base_model import UNSET
from .autogenerated_graphql_client.fragments import AddressValidityFields
from .config import Settings
from .exceptions import MultipleObjectsReturnedException
from .exceptions import UUIDNotFoundException

logger = structlog.stdlib.get_logger()


class Validity(Protocol):
    @property
    def from_(self) -> datetime | None:  # pragma: no cover
        ...

    @property
    def to(self) -> datetime | None:  # pragma: no cover
        ...


class ValidityModel(Protocol):
    @property
    def validity(self) -> Validity:  # pragma: no cover
        ...


T = TypeVar("T", bound=ValidityModel)


def extract_current_or_latest_validity(validities: list[T]) -> T | None:
    """
    Check each validity in a list of validities and return the one which is either
    valid today, or has the latest end-date
    """
    if len(validities) <= 1:
        return only(validities)

    def is_current(val: T) -> bool:
        # Cannot use datetime.utcnow as it is not timezone aware
        now_utc = datetime.now(UTC)

        match (val.validity.from_, val.validity.to):
            case (None, None):
                return True
            case (start, None):
                assert start is not None
                return start < now_utc
            case (None, end):
                assert end is not None
                return now_utc < end
            case (start, end):
                assert start is not None
                assert end is not None
                return start < now_utc < end
            case _:  # pragma: no cover
                raise AssertionError()

    # If any of the validities is valid today, return it
    current_validity = only(filter(is_current, validities))
    if current_validity:
        return current_validity
    # Otherwise return the latest
    # TODO: Does this actually make sense? - Should we not return the one which is the
    #       closest to now, rather than the one that is the furthest into the future?
    # Cannot use datetime.max directly as it is not timezone aware
    datetime_max_utc = datetime.max.replace(tzinfo=UTC)
    latest_validity = max(
        validities, key=lambda val: val.validity.to or datetime_max_utc
    )
    return latest_validity


AddressValidity = TypeVar("AddressValidity", bound=AddressValidityFields)


def graphql_address_to_ramodels_address(
    validities: list[AddressValidity],
) -> Address | None:
    result_entry = extract_current_or_latest_validity(validities)
    if result_entry is None:  # pragma: no cover
        return None
    entry = jsonable_encoder(result_entry)
    return Address(
        uuid=entry["uuid"],
        value=entry["value"],
        value2=entry["value2"],
        address_type=entry["address_type"]["uuid"],
        person=entry["employee_uuid"],
        org_unit=entry["org_unit_uuid"],
        engagement=entry["engagement_uuid"],
        visibility=entry["visibility_uuid"],
        validity=entry["validity"],
    )


Tc = TypeVar("Tc", covariant=True)


class HasValidities(Protocol[Tc]):
    @property
    def validities(self) -> Sequence[Tc]:  # pragma: no cover
        ...


class HasObjects(Protocol[Tc]):
    @property
    def objects(self) -> Sequence[Tc]:  # pragma: no cover
        ...


def flatten_validities(
    response: HasObjects[HasValidities[Tc]],
) -> Generator[Tc, None, None]:
    for obj in response.objects:
        yield from obj.validities


async def get_primary_engagement(
    graphql_client: GraphQLClient, uuid: EmployeeUUID
) -> UUID | None:
    """Decide the best primary engagement for the provided user.

    Args:
        uuid: UUID of the user to find the primary engagement for.

    Raises:
        RequeueMessage: If the method wants to wait for calculate_primary to run.

    Returns:
        The UUID of an engagement if found, otherwise None.
    """
    # TODO: Implement suppport for selecting primary engagements directly from MO
    # Get engagements from MO
    result = await graphql_client.read_engagements_is_primary(
        EngagementFilter(
            employee=EmployeeFilter(uuids=[uuid]), from_date=None, to_date=None
        )
    )
    # Flatten all validities to a list
    validities = list(flatten_validities(result))
    # No validities --> no primary
    if not validities:
        logger.info("No engagement validities found")
        return None

    # Remove all non-primary validities
    # This should contain a list of non-overlapping primary engagement validities,
    # assuming that primary calculation has run succesfully, overlaps indicate that
    # calculate_primary has not done its job correctly.
    # TODO: Check this invariant and throw RequeueMessage whenever it is broken?
    primary_validities = [val for val in validities if val.is_primary]

    # If there is validities, but none of them are primary, we need to wait for
    # calculate_primary to determine which validities are supposed to be primary.
    # TODO: Consider if we actually care to wait, we could just return `None` and
    #       notify that there is no primary while waiting for another AMQP message
    #       to come in, whenever calculate_primary has made changes.
    #       This however requires the engagement listener to actually trigger all
    #       code-paths that may end up calling this function.
    #       So for now we play it safe and keep this AMQP event around by requeuing.
    if validities and not primary_validities:
        logger.info(
            "Waiting for primary engagement to be decided",
            validities=validities,
            primary_validities=[],
        )
        raise RequeueMessage("Waiting for primary engagement to be decided")

    try:
        primary_engagement_validity = extract_current_or_latest_validity(
            primary_validities
        )
    except ValueError as e:
        # Multiple current primary engagements found, we cannot handle this
        # situation gracefully, so we requeue until calculate_primary resolves it.
        # NOTE: There may in fact still be multiple primary engagements in the past
        #       or future, but these are resolved by simply picking the latest one.
        # TODO: This should probably be fixed so we detect all overlaps
        logger.warning(
            "Waiting for multiple primary engagements to be resolved",
            validities=validities,
            primary_validities=primary_validities,
        )
        raise RequeueMessage(
            "Waiting for multiple primary engagements to be resolved"
        ) from e

    # No primary engagement identified, not even a delete/past ones
    # This should never occur since we check for primary_validities before calling
    # the extract_current_or_latest_object function. See the TODO for this check.
    # TODO: If we end up removing that check, then we should probably log and
    #       return None here instead of asserting it never happens.
    assert primary_engagement_validity is not None

    primary_engagement_uuid = primary_engagement_validity.uuid

    logger.info(
        "Found primary engagement",
        validities=validities,
        primary_validities=primary_validities,
        primary_engagement_uuid=primary_engagement_uuid,
    )
    return primary_engagement_uuid


class MOAPI:
    def __init__(self, settings: Settings, graphql_client: GraphQLClient) -> None:
        self.settings = settings
        self.graphql_client = graphql_client

    async def find_mo_employee_uuid_via_ituser(self, unique_uuid: UUID) -> set[UUID]:
        result = await self.graphql_client.read_employee_uuid_by_ituser_user_key(
            str(unique_uuid)
        )
        return {
            ituser.current.employee_uuid
            for ituser in result.objects
            if ituser.current is not None and ituser.current.employee_uuid is not None
        }

    async def get_it_system_uuid(self, itsystem_user_key: str) -> str:
        result = await self.graphql_client.read_itsystem_uuid(itsystem_user_key)
        exception = UUIDNotFoundException(
            f"itsystem not found, user_key: {itsystem_user_key}"
        )
        return str(one(result.objects, too_short=exception).uuid)

    async def load_mo_employee(
        self, uuid: UUID, current_objects_only=True
    ) -> Employee | None:
        start = end = UNSET if current_objects_only else None
        results = await self.graphql_client.read_employees([uuid], start, end)
        result = only(results.objects)
        if result is None:
            return None
        result_entry = extract_current_or_latest_validity(result.validities)
        if result_entry is None:
            return None
        entry = jsonable_encoder(result_entry)
        entry.pop("validity")
        return Employee(**entry)

    async def get_ldap_it_system_uuid(self) -> str | None:
        """
        Return the IT system uuid belonging to the LDAP-it-system
        Return None if the LDAP-it-system is not found.
        """
        if self.settings.ldap_it_system is None:
            return None

        try:
            return await self.get_it_system_uuid(self.settings.ldap_it_system)
        except UUIDNotFoundException:
            logger.info(
                "UUID Not found",
                suggestion=f"Does the '{self.settings.ldap_it_system}' it-system exist?",
            )
            return None

    async def load_mo_class_uuid(self, user_key: str) -> UUID | None:
        """Find the UUID of a class by user-key.

        Args:
            user_key: The user-key to lookup.

        Raises:
            MultipleObjectsReturnedException:
                If multiple classes share the same user-key.

        Returns:
            The UUID of the class or None if not found.
        """
        result = await self.graphql_client.read_class_uuid(user_key)
        too_long = MultipleObjectsReturnedException(
            f"Found multiple classes with user_key = '{user_key}': {result}"
        )
        klass = only(result.objects, too_long=too_long)
        if klass is None:
            return None
        return klass.uuid

    async def load_mo_facet_uuid(self, user_key: str) -> UUID | None:
        """Find the UUID of a facet by user-key.

        Args:
            user_key: The user-key to lookup.

        Raises:
            MultipleObjectsReturnedException:
                If multiple facets share the same user-key.

        Returns:
            The uuid of the facet or None if not found.
        """
        result = await self.graphql_client.read_facet_uuid(user_key)
        too_long = MultipleObjectsReturnedException(
            f"Found multiple facets with user_key = '{user_key}': {result}"
        )
        facet = only(result.objects, too_long=too_long)
        if facet is None:
            return None
        return facet.uuid

    async def load_mo_it_user(
        self, uuid: UUID, current_objects_only=True
    ) -> ITUser | None:
        start = end = UNSET if current_objects_only else None
        results = await self.graphql_client.read_itusers([uuid], start, end)
        result = only(results.objects)
        if result is None:
            return None
        result_entry = extract_current_or_latest_validity(result.validities)
        if result_entry is None:
            return None
        entry = jsonable_encoder(result_entry)
        it_user = ITUser.from_simplified_fields(
            user_key=entry["user_key"],
            itsystem_uuid=entry["itsystem_uuid"],
            from_date=entry["validity"]["from"],
            uuid=uuid,
            to_date=entry["validity"]["to"],
            person_uuid=entry["employee_uuid"],
            engagement_uuid=entry["engagement_uuid"],
        )
        # from_simplified_fields() has bad type annotation
        assert isinstance(it_user, ITUser)
        return it_user

    async def load_mo_address(
        self, uuid: UUID, current_objects_only: bool = True
    ) -> Address | None:
        """
        Loads a mo address

        Notes
        ---------
        Only returns addresses which are valid today. Meaning the to/from date is valid.
        """
        logger.info("Loading address", uuid=uuid)

        start = end = UNSET if current_objects_only else None
        results = await self.graphql_client.read_addresses([uuid], start, end)
        result = only(results.objects)
        if result is None:  # pragma: no cover
            return None
        return graphql_address_to_ramodels_address(result.validities)

    # TODO: Offer this via a dataloader, and change calls to use that
    async def is_primaries(self, engagements: list[UUID]) -> list[bool]:
        engagements_set = set(engagements)
        result = await self.graphql_client.read_is_primary_engagements(
            list(engagements_set)
        )
        result_map = {
            obj.current.uuid: obj.current.is_primary
            for obj in result.objects
            if obj.current is not None
        }
        return [result_map.get(uuid, False) for uuid in engagements]

    async def load_mo_engagement(
        self,
        uuid: UUID,
        current_objects_only: bool = True,
    ) -> Engagement | None:
        start = end = UNSET if current_objects_only else None
        results = await self.graphql_client.read_engagements([uuid], start, end)
        result = only(results.objects)
        if result is None:
            return None
        result_entry = extract_current_or_latest_validity(result.validities)
        if result_entry is None:
            return None
        entry = jsonable_encoder(result_entry)
        return Engagement(
            uuid=uuid,
            user_key=entry["user_key"],
            org_unit=entry["org_unit_uuid"],
            person=entry["employee_uuid"],
            job_function=entry["job_function_uuid"],
            engagement_type=entry["engagement_type_uuid"],
            primary=entry["primary_uuid"],
            extension_1=entry["extension_1"],
            extension_2=entry["extension_2"],
            extension_3=entry["extension_3"],
            extension_4=entry["extension_4"],
            extension_5=entry["extension_5"],
            extension_6=entry["extension_6"],
            extension_7=entry["extension_7"],
            extension_8=entry["extension_8"],
            extension_9=entry["extension_9"],
            extension_10=entry["extension_10"],
            validity=entry["validity"],
        )

    async def load_mo_employee_addresses(
        self, employee_uuid: UUID, address_type_uuid: UUID
    ) -> list[Address]:
        """
        Loads all current addresses of a specific type for an employee
        """
        result = await self.graphql_client.read_employee_addresses(
            employee_uuid, address_type_uuid
        )
        output = {
            obj.uuid: graphql_address_to_ramodels_address(obj.validities)
            for obj in result.objects
        }
        # If no active validities, pretend we did not get the object at all
        no_validity, validity = partition(
            star(lambda _, address: address), output.items()
        )
        no_validity_uuids = [uuid for uuid, _ in no_validity]
        if no_validity_uuids:  # pragma: no cover
            logger.warning(
                "Unable to lookup employee addresses", uuids=no_validity_uuids
            )
        return cast(list[Address], [obj for _, obj in validity])

    async def load_mo_org_unit_addresses(
        self, org_unit_uuid: OrgUnitUUID, address_type_uuid: UUID
    ) -> list[Address]:
        """
        Loads all current addresses of a specific type for an org unit
        """
        result = await self.graphql_client.read_org_unit_addresses(
            org_unit_uuid, address_type_uuid
        )
        output = {
            obj.uuid: graphql_address_to_ramodels_address(obj.validities)
            for obj in result.objects
        }
        # If no active validities, pretend we did not get the object at all
        no_validity, validity = partition(
            star(lambda _, address: address), output.items()
        )
        no_validity_uuids = [uuid for uuid, _ in no_validity]
        if no_validity_uuids:
            logger.warning(
                "Unable to lookup org-unit addresses", uuids=no_validity_uuids
            )
        return cast(list[Address], [obj for _, obj in validity])

    async def load_mo_employee_it_users(
        self,
        employee_uuid: UUID,
        it_system_uuid: UUID,
    ) -> list[ITUser]:
        """
        Load all current it users of a specific type linked to an employee
        """
        result = await self.graphql_client.read_ituser_by_employee_and_itsystem_uuid(
            employee_uuid, it_system_uuid
        )
        ituser_uuids = [ituser.uuid for ituser in result.objects]
        output = await asyncio.gather(*map(self.load_mo_it_user, ituser_uuids))
        # If no active validities, pretend we did not get the object at all
        output = [obj for obj in output if obj is not None]
        return cast(list[ITUser], output)

    async def load_mo_employee_engagement_dicts(
        self,
        employee_uuid: UUID,
        user_key: str | None = None,
    ) -> list[dict]:
        filter = EngagementFilter(employee=EmployeeFilter(uuids=[employee_uuid]))
        if user_key is not None:
            filter.user_keys = [user_key]

        result = await self.graphql_client.read_engagements_by_engagements_filter(
            filter
        )
        output = [
            jsonable_encoder(engagement.current)
            for engagement in result.objects
            if engagement.current
        ]
        return output

    async def load_mo_employee_engagements(
        self, employee_uuid: UUID
    ) -> list[Engagement]:
        """
        Load all current engagements linked to an employee
        """
        result = await self.graphql_client.read_engagements_by_employee_uuid(
            employee_uuid
        )
        engagement_uuids = [
            engagement.current.uuid
            for engagement in result.objects
            if engagement.current is not None
        ]
        output = await asyncio.gather(*map(self.load_mo_engagement, engagement_uuids))
        # If no active validities, pretend we did not get the object at all
        output = [obj for obj in output if obj is not None]
        return cast(list[Engagement], output)
