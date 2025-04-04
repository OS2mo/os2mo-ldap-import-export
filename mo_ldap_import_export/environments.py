# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import string
from contextlib import suppress
from datetime import datetime
from functools import partial
from typing import Any
from typing import TypeVar
from typing import cast
from uuid import UUID
from uuid import uuid4

import structlog
from fastapi.encoders import jsonable_encoder
from fastramqpi.ramqp.utils import RequeueMessage
from jinja2 import Environment
from jinja2 import StrictUndefined
from jinja2 import TemplateRuntimeError
from jinja2 import UndefinedError
from jinja2.utils import missing
from ldap3.utils.dn import safe_dn
from ldap3.utils.dn import to_dn
from more_itertools import flatten
from more_itertools import one
from more_itertools import only
from more_itertools import unzip
from pydantic import parse_obj_as

from mo_ldap_import_export.ldap import get_ldap_object
from mo_ldap_import_export.moapi import MOAPI
from mo_ldap_import_export.moapi import extract_current_or_latest_validity
from mo_ldap_import_export.moapi import flatten_validities
from mo_ldap_import_export.moapi import get_primary_engagement
from mo_ldap_import_export.models import Address
from mo_ldap_import_export.models import Engagement
from mo_ldap_import_export.models import ITUser

from .autogenerated_graphql_client.client import GraphQLClient
from .autogenerated_graphql_client.input_types import AddressFilter
from .autogenerated_graphql_client.input_types import ClassFilter
from .autogenerated_graphql_client.input_types import EmployeeFilter
from .autogenerated_graphql_client.input_types import EngagementFilter
from .autogenerated_graphql_client.input_types import ITSystemFilter
from .autogenerated_graphql_client.input_types import ITUserFilter
from .autogenerated_graphql_client.input_types import OrganisationUnitFilter
from .autogenerated_graphql_client.input_types import OrgUnitsboundmanagerfilter
from .config import Settings
from .dataloaders import DataLoader
from .exceptions import NoObjectsReturnedException
from .exceptions import SkipObject
from .exceptions import UUIDNotFoundException
from .types import DN
from .types import EmployeeUUID
from .types import EngagementUUID
from .utils import MO_TZ
from .utils import ensure_list
from .utils import extract_ou_from_dn
from .utils import get_delete_flag
from .utils import mo_today

logger = structlog.stdlib.get_logger()
T = TypeVar("T")


def filter_mo_datestring(datetime_object):
    """
    Converts a datetime object to a date string which is accepted by MO.

    Notes
    -------
    MO only accepts date objects dated at midnight.
    """
    # TODO: should take timezone-aware datetime_object and convert using MO_TZ.
    if not datetime_object:
        return None
    return datetime_object.strftime("%Y-%m-%dT00:00:00")


def filter_strip_non_digits(input_string):
    if not isinstance(input_string, str):
        return None
    return "".join(c for c in input_string if c in string.digits)


def filter_remove_curly_brackets(text: str) -> str:
    # TODO: Should this remove everything or just a single set?
    return text.replace("{", "").replace("}", "")


def bitwise_and(input: int, bitmask: int) -> int:
    """Bitwise and jinja filter.

    Mostly useful for accessing bits within userAccountControl.

    Args:
        input: The input integer.
        bitmask: The bitmask to filter the input through.

    Returns:
        The bitwise and on input and bitmask.
    """
    return input & bitmask


async def _get_facet_class_uuid(
    graphql_client: GraphQLClient, class_user_key: str, facet_user_key: str
) -> str:
    result = await graphql_client.read_class_uuid_by_facet_and_class_user_key(
        facet_user_key, class_user_key
    )
    exception = UUIDNotFoundException(
        f"class not found, facet_user_key: {facet_user_key} class_user_key: {class_user_key}"
    )
    return str(one(result.objects, too_short=exception).uuid)


get_employee_address_type_uuid = partial(
    _get_facet_class_uuid, facet_user_key="employee_address_type"
)
get_visibility_uuid = partial(_get_facet_class_uuid, facet_user_key="visibility")
get_org_unit_type_uuid = partial(_get_facet_class_uuid, facet_user_key="org_unit_type")


async def get_org_unit_path_string(
    graphql_client: GraphQLClient, org_unit_path_string_separator: str, uuid: str | UUID
) -> str:
    uuid = uuid if isinstance(uuid, UUID) else UUID(uuid)
    result = await graphql_client.read_org_unit_ancestor_names(uuid)
    current = one(result.objects).current
    assert current is not None
    names = [x.name for x in reversed(current.ancestors)] + [current.name]
    assert org_unit_path_string_separator not in names
    return org_unit_path_string_separator.join(names)


# TODO: Clean this up so it always just takes an UUID
async def get_org_unit_name_for_parent(
    graphql_client: GraphQLClient, uuid: UUID | str, layer: int = 0
) -> str | None:
    """Get the name of the ancestor in the n'th layer of the org tree.

    Example:

        Imagine an org-unit tree like the following:
            ```
            └── Kolding Kommune
                └── Sundhed
                    ├── Plejecentre
                    │   ├── Plejecenter Nord
                    │   │   └── Køkken <-- uuid of this provided
                    │   └── Plejecenter Syd
                    │       └── Køkken
                    └── Teknik
            ```

        Calling this function with the uuid above and layer, would return:

        * 0: "Kolding Kommune"
        * 1: "Sundhed"
        * 2: "Plejecentre"
        * 3: "Plejecenter Nord"
        * 4: "Køkken"
        * n: ""

    Args:
        graphql_client: GraphQLClient to fetch org-units from MO with.
        uuid: Organisation Unit UUID of the org-unit to find ancestors of.
        layer: The layer the ancestor to extract is on.

    Returns:
        The name of the ancestor at the n'th layer above the provided org-unit.
        If the layer provided is beyond the depth available None is returned.
    """
    uuid = uuid if isinstance(uuid, UUID) else UUID(uuid)
    result = await graphql_client.read_org_unit_ancestor_names(uuid)
    current = one(result.objects).current
    assert current is not None
    names = [x.name for x in reversed(current.ancestors)] + [current.name]
    with suppress(IndexError):
        return names[layer]
    return None


async def get_job_function_name(graphql_client: GraphQLClient, uuid: UUID) -> str:
    result = await graphql_client.read_class_name_by_class_uuid(uuid)
    job_function = one(result.objects)
    if job_function.current is None:
        raise NoObjectsReturnedException(f"job_function not active, uuid: {uuid}")
    return job_function.current.name


async def get_org_unit_name(graphql_client: GraphQLClient, uuid: UUID) -> str:
    result = await graphql_client.read_org_unit_name(uuid)
    org_unit = one(result.objects)
    if org_unit.current is None:
        raise NoObjectsReturnedException(f"org_unit not active, uuid: {uuid}")
    return org_unit.current.name


async def _create_facet_class(
    moapi: MOAPI, class_user_key: str, facet_user_key: str
) -> UUID:
    """Creates a class under the specified facet in MO.

    Args:
        dataloader: Our dataloader instance
        facet_user_key: User-key of the facet to create the class under.
        class_user_key: The name/user-key to give the class.

    Returns:
        The uuid of the created class
    """
    logger.info("Creating MO class", facet_user_key=facet_user_key, name=class_user_key)
    facet_uuid = await moapi.load_mo_facet_uuid(facet_user_key)
    if facet_uuid is None:
        raise NoObjectsReturnedException(
            f"Could not find facet with user_key = '{facet_user_key}'"
        )
    return await moapi.create_mo_class(
        name=class_user_key, user_key=class_user_key, facet_uuid=facet_uuid
    )


async def _get_or_create_facet_class(
    moapi: MOAPI,
    class_user_key: str,
    facet_user_key: str,
    default: str | None = None,
) -> str:
    if not class_user_key:
        if default is None:
            raise UUIDNotFoundException("Cannot create class without user-key")
        logger.info("class_user_key is empty, using provided default", default=default)
        class_user_key = default
    try:
        return await _get_facet_class_uuid(
            moapi.graphql_client,
            class_user_key=class_user_key,
            facet_user_key=facet_user_key,
        )
    except UUIDNotFoundException:
        uuid = await _create_facet_class(
            moapi,
            class_user_key=class_user_key,
            facet_user_key=facet_user_key,
        )
        return str(uuid)


get_or_create_job_function_uuid = partial(
    _get_or_create_facet_class, facet_user_key="engagement_job_function"
)


async def load_primary_engagement(
    moapi: MOAPI, employee_uuid: UUID
) -> Engagement | None:
    primary_engagement_uuid = await get_primary_engagement(
        moapi.graphql_client, EmployeeUUID(employee_uuid)
    )
    if primary_engagement_uuid is None:
        logger.info(
            "Could not find primary engagement UUID", employee_uuid=employee_uuid
        )
        return None

    fetched_engagement = await moapi.load_mo_engagement(
        primary_engagement_uuid, start=None, end=None
    )
    if fetched_engagement is None:  # pragma: no cover
        logger.error("Unable to load mo engagement", uuid=primary_engagement_uuid)
        raise RequeueMessage("Unable to load mo engagement")
    delete = get_delete_flag(jsonable_encoder(fetched_engagement))
    if delete:
        logger.debug("Primary engagement is terminated", uuid=primary_engagement_uuid)
        return None
    return fetched_engagement


async def load_it_user(
    moapi: MOAPI,
    employee_uuid: UUID,
    itsystem_user_key: str,
    return_terminated: bool = False,
) -> ITUser | None:
    result = await moapi.graphql_client.read_filtered_itusers(
        ITUserFilter(
            employee=EmployeeFilter(uuids=[employee_uuid]),
            itsystem=ITSystemFilter(user_keys=[itsystem_user_key]),
            from_date=None,
            to_date=None,
        )
    )
    if not result.objects:
        logger.info(
            "Could not find it-user",
            employee_uuid=employee_uuid,
            itsystem_user_key=itsystem_user_key,
        )
        return None
    # Flatten all validities to a list
    validities = list(flatten_validities(result))
    validity = extract_current_or_latest_validity(validities)
    if validity is None:  # pragma: no cover
        logger.error(
            "No active validities on it-user",
            employee_uuid=employee_uuid,
            itsystem_user_key=itsystem_user_key,
        )
        raise RequeueMessage("No active validities on it-user")
    fetched_ituser = await moapi.load_mo_it_user(
        validity.uuid, current_objects_only=False
    )
    if fetched_ituser is None:  # pragma: no cover
        logger.error("Unable to load it-user", uuid=validity.uuid)
        raise RequeueMessage("Unable to load it-user")
    # If allowed to return terminated, there is no reason to check for it
    # we simply return whatever we found and use that
    if return_terminated:
        return fetched_ituser
    delete = get_delete_flag(jsonable_encoder(fetched_ituser))
    if delete:
        logger.debug("IT-user is terminated", uuid=validity.uuid)
        return None
    return fetched_ituser


async def create_mo_it_user(
    moapi: MOAPI, employee_uuid: UUID, itsystem_user_key: str, user_key: str
) -> ITUser | None:
    it_system_uuid = UUID(await moapi.get_it_system_uuid(itsystem_user_key))

    # Make a new it-user
    it_user = ITUser(
        user_key=user_key,
        itsystem=it_system_uuid,
        person=employee_uuid,
        validity={"start": mo_today()},
    )
    await moapi.create_ituser(it_user)
    return await load_it_user(moapi, employee_uuid, itsystem_user_key)


async def load_address(
    moapi: MOAPI, employee_uuid: UUID, address_type_user_key: str
) -> Address | None:
    result = await moapi.graphql_client.read_filtered_addresses(
        AddressFilter(
            employee=EmployeeFilter(uuids=[employee_uuid]),
            address_type=ClassFilter(user_keys=[address_type_user_key]),
            from_date=None,
            to_date=None,
        )
    )
    if not result.objects:
        logger.info(
            "Could not find employee address",
            employee_uuid=employee_uuid,
            address_type_user_key=address_type_user_key,
        )
        return None
    # Flatten all validities to a list
    validities = list(flatten_validities(result))
    validity = extract_current_or_latest_validity(validities)
    if validity is None:  # pragma: no cover
        logger.error(
            "No active validities on employee address",
            employee_uuid=employee_uuid,
            address_type_user_key=address_type_user_key,
        )
        raise RequeueMessage("No active validities on employee address")
    fetched_address = await moapi.load_mo_address(
        validity.uuid, current_objects_only=False
    )
    if fetched_address is None:  # pragma: no cover
        logger.error("Unable to load employee address", uuid=validity.uuid)
        raise RequeueMessage("Unable to load employee address")
    delete = get_delete_flag(jsonable_encoder(fetched_address))
    if delete:
        logger.debug("Employee address is terminated", uuid=validity.uuid)
        return None
    return fetched_address


async def load_org_unit_address(
    moapi: MOAPI, employee_uuid: UUID, address_type_user_key: str
) -> Address | None:
    primary_engagement_uuid = await get_primary_engagement(
        moapi.graphql_client, EmployeeUUID(employee_uuid)
    )
    if primary_engagement_uuid is None:
        logger.info(
            "Could not find primary engagement UUID", employee_uuid=employee_uuid
        )
        return None

    result = await moapi.graphql_client.read_filtered_addresses(
        AddressFilter(
            # TODO: Use primary engagement filter here
            org_unit=OrganisationUnitFilter(
                engagement=EngagementFilter(uuids=[primary_engagement_uuid])
            ),
            address_type=ClassFilter(user_keys=[address_type_user_key]),
            from_date=None,
            to_date=None,
        )
    )
    validities = list(flatten(o.validities for o in result.objects))
    validity = extract_current_or_latest_validity(validities)
    if validity is None:
        logger.error(
            "No active validities on org-unit address",
            employee_uuid=employee_uuid,
            address_type_user_key=address_type_user_key,
        )
        return None
    fetched_address = await moapi.load_mo_address(
        validity.uuid, current_objects_only=False
    )
    if fetched_address is None:  # pragma: no cover
        logger.error("Unable to load org-unit address", uuid=validity.uuid)
        raise RequeueMessage("Unable to load org-unit address")
    delete = get_delete_flag(jsonable_encoder(fetched_address))
    if delete:
        logger.debug("Org-unit address is terminated", uuid=validity.uuid)
        return None
    return fetched_address


async def generate_username(
    dataloader: DataLoader,
    employee_uuid: UUID,
) -> str:
    employee = await dataloader.moapi.load_mo_employee(employee_uuid)
    if employee is None:  # pragma: no cover
        raise NoObjectsReturnedException(f"Unable to lookup employee: {employee_uuid}")
    return cast(str, await dataloader.username_generator.generate_username(employee))


async def generate_common_name(
    dataloader: DataLoader,
    employee_uuid: UUID,
    dn: DN,
) -> str:
    # Fetch the current common name (if any)
    ldap_connection = dataloader.ldapapi.ldap_connection
    current_common_name = None
    with suppress(NoObjectsReturnedException):
        ldap_object = await get_ldap_object(ldap_connection, dn, {"cn"})
        ldap_common_name = getattr(ldap_object, "cn", None)
        if ldap_common_name is not None:
            # This is a list on OpenLDAP, but not on AD
            # We use ensure_list to ensure that AD is handled like Standard LDAP
            current_common_name = one(ensure_list(ldap_common_name))

    employee = await dataloader.moapi.load_mo_employee(employee_uuid)
    if employee is None:  # pragma: no cover
        raise NoObjectsReturnedException(f"Unable to lookup employee: {employee_uuid}")
    return cast(
        str,
        await dataloader.username_generator.generate_common_name(
            employee, current_common_name
        ),
    )


async def get_address_uuid(
    graphql_client: GraphQLClient, filter: dict[str, Any]
) -> UUID | None:
    address_filter = parse_obj_as(AddressFilter, filter)
    result = await graphql_client.read_address_uuid(address_filter)
    obj = only(result.objects)
    return obj.uuid if obj else None


async def get_ituser_uuid(
    graphql_client: GraphQLClient, filter: dict[str, Any]
) -> UUID | None:
    ituser_filter = parse_obj_as(ITUserFilter, filter)
    result = await graphql_client.read_ituser_uuid(ituser_filter)
    obj = only(result.objects)
    return obj.uuid if obj else None


async def get_engagement_uuid(
    graphql_client: GraphQLClient, filter: dict[str, Any]
) -> UUID | None:
    engagement_filter = parse_obj_as(EngagementFilter, filter)
    result = await graphql_client.read_engagement_uuid(engagement_filter)
    obj = only(result.objects)
    return obj.uuid if obj else None


async def get_org_unit_uuid(
    graphql_client: GraphQLClient, filter: dict[str, Any]
) -> UUID | None:
    org_unit_filter = parse_obj_as(OrganisationUnitFilter, filter)
    result = await graphql_client.read_org_unit_uuid(org_unit_filter)
    obj = only(result.objects)
    return obj.uuid if obj else None


async def get_employment_interval(
    graphql_client: GraphQLClient, employee_uuid: UUID
) -> tuple[datetime | None, datetime | None]:
    result = await graphql_client.read_engagement_enddate(employee_uuid)
    if not result.objects:
        return None, None

    tzmin = datetime.min.replace(tzinfo=MO_TZ)
    tzmax = datetime.max.replace(tzinfo=MO_TZ)

    start_dates, end_dates = unzip(
        (validity.validity.from_ or tzmin, validity.validity.to or tzmax)
        for engagement in result.objects
        for validity in engagement.validities
    )
    startdate = min(start_dates)
    enddate = max(end_dates)
    return startdate, enddate


async def get_manager_person_uuid(
    graphql_client: GraphQLClient,
    engagement_uuid: EngagementUUID,
    filter: dict[str, Any] | None = None,
) -> UUID | None:
    manager_filter = None
    if filter:
        manager_filter = parse_obj_as(OrgUnitsboundmanagerfilter, filter)
    result = await graphql_client.read_engagement_manager(
        engagement_uuid, manager_filter
    )

    obj = only(result.objects)
    if obj is None:
        logger.debug("Invalid engagement", engagement_uuid=engagement_uuid)
        return None

    current = obj.current
    # Our lookup is specifically for current engagements
    assert current is not None

    # NOTE: We assume that there is at most one manager in managers, as any others
    #       should have have been filtered using the manager filter.
    manager = only(current.managers)
    if manager is None:
        logger.debug(
            "No manager relation found",
            engagement_uuid=engagement_uuid,
            manager_filter=filter,
        )
        return None

    # NOTE: manager.person may be null if we hit a vacant manager position
    #       The caller can avoid this, by setting `employee: null` on the manager filter.
    if manager.person is None:
        logger.debug(
            "Vacant manager found",
            engagement_uuid=engagement_uuid,
            manager_filter=filter,
        )
        return None

    manager_validity = one(manager.person)
    return manager_validity.uuid


async def get_person_dn(dataloader: DataLoader, uuid: EmployeeUUID) -> DN | None:
    dn, create = await dataloader._find_best_dn(uuid, dry_run=True)
    if create:
        logger.debug(
            "_find_best_dn returned create=True in get_person_dn", employee_uuid=uuid
        )
        return None
    return dn


def skip_if_none(obj: T | None) -> T:
    if obj is None:
        raise SkipObject("Skipping: Object is None")
    return obj


def requeue_if_none(obj: T | None) -> T:
    if obj is None:
        raise RequeueMessage("Requeueing: Object is None")
    return obj


def parent_dn(dn: DN) -> DN:
    dn_parts = to_dn(dn)
    parent_dn_parts = dn_parts[1:]
    return cast(DN, safe_dn(parent_dn_parts))


def dn_has_ou(dn: DN) -> bool:
    return bool(extract_ou_from_dn(dn))


def construct_globals_dict(
    settings: Settings, dataloader: DataLoader
) -> dict[str, Any]:
    moapi = dataloader.moapi
    graphql_client = moapi.graphql_client
    return {
        "get_employee_address_type_uuid": partial(
            get_employee_address_type_uuid, graphql_client
        ),
        "get_it_system_uuid": partial(moapi.get_it_system_uuid),
        "get_visibility_uuid": partial(get_visibility_uuid, graphql_client),
        "get_org_unit_type_uuid": partial(get_org_unit_type_uuid, graphql_client),
        "get_org_unit_path_string": partial(
            get_org_unit_path_string,
            graphql_client,
            settings.org_unit_path_string_separator,
        ),
        "get_org_unit_name_for_parent": partial(
            get_org_unit_name_for_parent, graphql_client
        ),
        "get_job_function_name": partial(get_job_function_name, graphql_client),
        "get_org_unit_name": partial(get_org_unit_name, graphql_client),
        "get_or_create_job_function_uuid": partial(
            get_or_create_job_function_uuid, moapi
        ),
        # These names are intentionally bad, but consistent with the old code names
        # TODO: Rename these functions once the old template system is gone
        "load_mo_employee": moapi.load_mo_employee,
        "load_mo_primary_engagement": partial(load_primary_engagement, moapi),
        "load_mo_it_user": partial(load_it_user, moapi),
        "load_mo_address": partial(load_address, moapi),
        "load_mo_org_unit_address": partial(load_org_unit_address, moapi),
        "create_mo_it_user": partial(create_mo_it_user, moapi),
        "generate_username": partial(generate_username, dataloader),
        "generate_common_name": partial(generate_common_name, dataloader),
        "get_address_uuid": partial(get_address_uuid, graphql_client),
        "get_ituser_uuid": partial(get_ituser_uuid, graphql_client),
        "get_engagement_uuid": partial(get_engagement_uuid, graphql_client),
        "get_org_unit_uuid": partial(get_org_unit_uuid, graphql_client),
        "get_employment_interval": partial(get_employment_interval, graphql_client),
        "get_manager_person_uuid": partial(get_manager_person_uuid, graphql_client),
        "get_person_dn": partial(get_person_dn, dataloader),
    }


class NeverUndefined(StrictUndefined):
    """https://github.com/pallets/jinja/issues/1923."""

    def __init__(
        self,
        hint: str | None = None,
        obj: Any = missing,
        name: str | None = None,
        exc: type[TemplateRuntimeError] = UndefinedError,
    ) -> None:
        raise Exception(
            f"Undefined variable '{name}' with object {obj} (hint: {hint})"
        ) from exc


def construct_default_environment() -> Environment:
    # We intentionally use 'StrictUndefined' here so undefined accesses yield exceptions
    # instead of silently coercing to falsy values as is the case with 'Undefined'
    # See: https://jinja.palletsprojects.com/en/3.1.x/api/#undefined-types
    # For more details.
    environment = Environment(undefined=NeverUndefined, enable_async=True)

    environment.filters["bitwise_and"] = bitwise_and
    environment.filters["mo_datestring"] = filter_mo_datestring
    environment.filters["strip_non_digits"] = filter_strip_non_digits
    environment.filters["remove_curly_brackets"] = filter_remove_curly_brackets

    environment.globals["now"] = datetime.utcnow  # TODO: timezone-aware datetime
    environment.globals["skip_if_none"] = skip_if_none
    environment.globals["requeue_if_none"] = requeue_if_none
    environment.globals["uuid4"] = uuid4
    environment.globals["parent_dn"] = parent_dn
    environment.globals["dn_has_ou"] = dn_has_ou

    return environment


def construct_environment(settings: Settings, dataloader: DataLoader) -> Environment:
    environment = construct_default_environment()
    environment.globals.update(construct_globals_dict(settings, dataloader))
    return environment
