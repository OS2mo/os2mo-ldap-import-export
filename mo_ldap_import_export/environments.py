# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import string
from collections.abc import Iterator
from contextlib import suppress
from datetime import datetime
from functools import partial
from itertools import compress
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
from more_itertools import flatten
from more_itertools import one
from more_itertools import only

from mo_ldap_import_export.moapi import extract_current_or_latest_validity
from mo_ldap_import_export.moapi import get_primary_engagement
from mo_ldap_import_export.models import Address
from mo_ldap_import_export.models import Engagement
from mo_ldap_import_export.models import ITUser
from mo_ldap_import_export.models import OrganisationUnit

from .autogenerated_graphql_client.client import GraphQLClient
from .autogenerated_graphql_client.input_types import AddressFilter
from .autogenerated_graphql_client.input_types import ClassFilter
from .autogenerated_graphql_client.input_types import EmployeeFilter
from .autogenerated_graphql_client.input_types import EngagementFilter
from .autogenerated_graphql_client.input_types import ITSystemFilter
from .autogenerated_graphql_client.input_types import ITUserFilter
from .autogenerated_graphql_client.input_types import OrganisationUnitFilter
from .config import Settings
from .dataloaders import DataLoader
from .exceptions import NoObjectsReturnedException
from .exceptions import SkipObject
from .exceptions import UUIDNotFoundException
from .types import EmployeeUUID
from .utils import exchange_ou_in_dn
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


def filter_splitfirst(text, separator=" "):
    """
    Splits a string at the first space, returning two elements
    This is convenient for splitting a name into a givenName and a surname
    and works for names with no spaces (surname will then be empty)
    """
    if text is not None:
        text = str(text)
        if text != "":
            s = text.split(separator, 1)
            return s if len(s) > 1 else (s + [""])
    return ["", ""]


def filter_splitlast(text, separator=" "):
    """
    Splits a string at the last space, returning two elements
    This is convenient for splitting a name into a givenName and a surname
    and works for names with no spaces (givenname will then be empty)
    """
    if text is not None:
        text = str(text)
        if text != "":
            text = str(text)
            s = text.split(separator)
            return [separator.join(s[:-1]), s[-1]]
    return ["", ""]


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
get_org_unit_type_uuid = partial(_get_facet_class_uuid, facet_user_key="org_unit_type")
get_org_unit_level_uuid = partial(
    _get_facet_class_uuid, facet_user_key="org_unit_level"
)
get_visibility_uuid = partial(_get_facet_class_uuid, facet_user_key="visibility")
get_primary_type_uuid = partial(_get_facet_class_uuid, facet_user_key="primary_type")
get_engagement_type_uuid = partial(
    _get_facet_class_uuid, facet_user_key="engagement_type"
)


async def load_mo_root_org_uuid(graphql_client: GraphQLClient) -> UUID:
    """Get the UUID of the root organisational unit in MO.

    Args:
        graphql_client: GraphQLClient to fetch root org from MO with.

    Returns:
        The UUID of the root organisational unit.
    """
    result = await graphql_client.read_root_org_uuid()
    return result.uuid


async def get_org_unit_uuid_from_path(
    graphql_client: GraphQLClient,
    org_unit_path: list[str],
) -> UUID:
    def construct_filter(names: Iterator[str]) -> OrganisationUnitFilter | None:
        name = next(names, None)
        if name is None:
            return None
        return OrganisationUnitFilter(names=[name], parent=construct_filter(names))

    filter = construct_filter(reversed(org_unit_path))
    assert filter is not None
    result = await graphql_client.read_org_unit_uuid(filter)
    obj = only(result.objects)
    if obj is None:
        raise UUIDNotFoundException(f"{org_unit_path} not found in OS2mo")
    return obj.uuid


async def create_org_unit(
    dataloader: DataLoader, settings: Settings, org_unit_path: list[str]
) -> UUID:
    """Create the org-unit and any missing parents in org_unit_path.

    The function works by recursively creating parents until an existing parent is
    found or we arrive at the root org.

    Args:
        org_unit_path: The org-unit path to ensure exists.

    Returns:
        UUID of the newly created org-unit.
    """
    # If asked to create the root org, simply return it
    if not org_unit_path:
        return await load_mo_root_org_uuid(dataloader.graphql_client)

    # If the org-unit path already exists, no need to create, simply return it
    with suppress(UUIDNotFoundException):
        return await get_org_unit_uuid_from_path(
            dataloader.graphql_client, org_unit_path
        )

    # If we get here, the path did not already exist, so we need to create it
    logger.info("Importing", path=org_unit_path)

    # Figure out our name and our parent path
    # Split the org-unit path into name and parent path
    # The last element is the name with all the rest coming before being the parent
    *parent_path, name = org_unit_path

    # Get or create our parent uuid (recursively)
    parent_uuid = await create_org_unit(dataloader, settings, parent_path)

    default_org_unit_type_uuid = UUID(
        await get_org_unit_type_uuid(
            dataloader.graphql_client, settings.default_org_unit_type
        )
    )
    default_org_unit_level_uuid = UUID(
        await get_org_unit_level_uuid(
            dataloader.graphql_client, settings.default_org_unit_level
        )
    )

    uuid = uuid4()
    org_unit = OrganisationUnit.from_simplified_fields(
        org_unit_type_uuid=default_org_unit_type_uuid,
        org_unit_level_uuid=default_org_unit_level_uuid,
        # Note: 1902 seems to be the earliest accepted year by OS2mo
        # We pick 1960 because MO's dummy data also starts all organizations
        # in 1960...
        # We just want a very early date here, to avoid that imported employee
        # engagements start before the org-unit existed.
        from_date="1960-01-01T00:00:00",
        # Org-unit specific fields
        user_key=str(uuid4()),
        name=name,
        parent_uuid=parent_uuid,
        uuid=uuid,
    )
    # from_simplified_fields() has bad type annotation
    assert isinstance(org_unit, OrganisationUnit)
    await dataloader.create_org_unit(org_unit)
    return uuid


async def get_engagement_type_name(graphql_client: GraphQLClient, uuid: UUID) -> str:
    result = await graphql_client.read_class_name_by_class_uuid(uuid)
    engagement_type = one(result.objects)
    if engagement_type.current is None:
        raise NoObjectsReturnedException(f"engagement_type not active, uuid: {uuid}")
    return engagement_type.current.name


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


def make_dn_from_org_unit_path(
    org_unit_path_string_separator: str, dn: str, org_unit_path_string: str
) -> str:
    """
    Makes a new DN based on an org-unit path string and a DN, where the org unit
    structure is parsed as an OU structure in the DN.

    Example
    --------
    >>> dn = "CN=Earthworm Jim,OU=OS2MO,DC=ad,DC=addev"
    >>> new_dn = make_dn_from_org_unit_path(dn,"foo/bar")
    >>> new_dn
    >>> "CN=Earthworm Jim,OU=bar,OU=foo,DC=ad,DC=addev"
    """
    sep = org_unit_path_string_separator

    org_units = org_unit_path_string.split(sep)[::-1]
    new_ou = ",".join([f"OU={org_unit.strip()}" for org_unit in org_units])
    return exchange_ou_in_dn(dn, new_ou)


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
    dataloader: DataLoader, class_user_key: str, facet_user_key: str
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
    facet_uuid = await dataloader.moapi.load_mo_facet_uuid(facet_user_key)
    if facet_uuid is None:
        raise NoObjectsReturnedException(
            f"Could not find facet with user_key = '{facet_user_key}'"
        )
    return await dataloader.create_mo_class(
        name=class_user_key, user_key=class_user_key, facet_uuid=facet_uuid
    )


async def _get_or_create_facet_class(
    dataloader: DataLoader,
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
            dataloader.graphql_client,
            class_user_key=class_user_key,
            facet_user_key=facet_user_key,
        )
    except UUIDNotFoundException:
        uuid = await _create_facet_class(
            dataloader, class_user_key=class_user_key, facet_user_key=facet_user_key
        )
        return str(uuid)


get_or_create_job_function_uuid = partial(
    _get_or_create_facet_class, facet_user_key="engagement_job_function"
)
get_or_create_engagement_type_uuid = partial(
    _get_or_create_facet_class, facet_user_key="engagement_type"
)


async def get_current_engagement_attribute_uuid_dict(
    dataloader: DataLoader,
    employee_uuid: UUID,
    engagement_user_key: str,
    attribute: str,
) -> dict[str, str]:
    """
    Returns an uuid-dictionary with the uuid matching the desired attribute

    Args:
        attribute: Attribute to look up.
            For example:
                - org_unit_uuid
                - engagement_type_uuid
                - primary_uuid
        employee_uuid: UUID of the employee
        engagement_user_key: user_key of the engagement

    Note:
        This method requests all engagements for employee with uuid = employee_uuid
        and then filters out all engagements which do not match engagement_user_key.
        If there is exactly one engagement left after this, the uuid of the requested
        attribute is returned.
    """

    if "uuid" not in attribute:
        raise ValueError(
            "attribute must be an uuid-string. For example 'job_function_uuid'"
        )

    logger.info(
        f"Looking for '{attribute}' in existing engagement with "
        f"user_key = '{engagement_user_key}' "
        f"and employee_uuid = '{employee_uuid}'"
    )
    engagement_dicts = await dataloader.moapi.load_mo_employee_engagement_dicts(
        employee_uuid, engagement_user_key
    )

    too_short_exception = UUIDNotFoundException(
        f"Employee with uuid = {employee_uuid} has no engagements "
        f"with user_key = '{engagement_user_key}'"
    )
    too_long_exception = UUIDNotFoundException(
        f"Employee with uuid = {employee_uuid} has multiple engagements "
        f"with user_key = '{engagement_user_key}'"
    )
    engagement = one(
        engagement_dicts, too_short=too_short_exception, too_long=too_long_exception
    )
    logger.info(f"Match found in engagement with uuid = {engagement['uuid']}")
    return {"uuid": engagement[attribute]}


get_current_org_unit_uuid_dict = partial(
    get_current_engagement_attribute_uuid_dict, attribute="org_unit_uuid"
)
get_current_engagement_type_uuid_dict = partial(
    get_current_engagement_attribute_uuid_dict, attribute="engagement_type_uuid"
)


async def get_current_primary_uuid_dict(
    dataloader: DataLoader, employee_uuid: UUID, engagement_user_key: str
) -> dict | None:
    """
    Returns an existing 'primary' object formatted as a dict
    """
    primary_dict = await get_current_engagement_attribute_uuid_dict(
        dataloader, employee_uuid, engagement_user_key, "primary_uuid"
    )

    if not primary_dict["uuid"]:
        return None
    return primary_dict


async def get_primary_engagement_dict(
    dataloader: DataLoader, employee_uuid: UUID
) -> dict:
    engagements = await dataloader.moapi.load_mo_employee_engagement_dicts(
        employee_uuid
    )
    # TODO: Make is_primary a GraphQL filter in MO and clean this up
    is_primary_engagement = await dataloader.moapi.is_primaries(
        [engagement["uuid"] for engagement in engagements]
    )
    primary_engagement = one(compress(engagements, is_primary_engagement))
    return primary_engagement


async def get_employee_dict(dataloader: DataLoader, employee_uuid: UUID) -> dict:
    mo_employee = await dataloader.moapi.load_mo_employee(employee_uuid)
    if mo_employee is None:
        raise NoObjectsReturnedException(f"Unable to lookup employee: {employee_uuid}")
    return mo_employee.dict()


async def load_primary_engagement(
    dataloader: DataLoader, employee_uuid: UUID
) -> Engagement | None:
    primary_engagement_uuid = await get_primary_engagement(
        dataloader.graphql_client, EmployeeUUID(employee_uuid)
    )
    if primary_engagement_uuid is None:
        logger.info(
            "Could not find primary engagement UUID", employee_uuid=employee_uuid
        )
        return None

    fetched_engagement = await dataloader.moapi.load_mo_engagement(
        primary_engagement_uuid, current_objects_only=False
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
    dataloader: DataLoader, employee_uuid: UUID, itsystem_user_key: str
) -> ITUser | None:
    result = await dataloader.graphql_client.read_filtered_itusers(
        ITUserFilter(
            employee=EmployeeFilter(uuids=[employee_uuid]),
            itsystem=ITSystemFilter(user_keys=[itsystem_user_key]),
            from_date=None,
            to_date=None,
        )
    )
    ituser = only(result.objects)
    if ituser is None:
        logger.info(
            "Could not find it-user",
            employee_uuid=employee_uuid,
            itsystem_user_key=itsystem_user_key,
        )
        return None
    validity = extract_current_or_latest_validity(ituser.validities)
    if validity is None:  # pragma: no cover
        logger.error(
            "No active validities on it-user",
            employee_uuid=employee_uuid,
            itsystem_user_key=itsystem_user_key,
        )
        raise RequeueMessage("No active validities on it-user")
    fetched_ituser = await dataloader.moapi.load_mo_it_user(
        validity.uuid, current_objects_only=False
    )
    if fetched_ituser is None:  # pragma: no cover
        logger.error("Unable to load it-user", uuid=validity.uuid)
        raise RequeueMessage("Unable to load it-user")
    delete = get_delete_flag(jsonable_encoder(fetched_ituser))
    if delete:
        logger.debug("IT-user is terminated", uuid=validity.uuid)
        return None
    return fetched_ituser


async def create_mo_it_user(
    dataloader: DataLoader, employee_uuid: UUID, itsystem_user_key: str, user_key: str
) -> ITUser | None:
    it_system_uuid = UUID(await dataloader.moapi.get_it_system_uuid(itsystem_user_key))

    # Make a new it-user
    it_user = ITUser(
        user_key=user_key,
        itsystem=it_system_uuid,
        person=employee_uuid,
        validity={"start": mo_today()},
    )
    await dataloader.create_ituser(it_user)
    return await load_it_user(dataloader, employee_uuid, itsystem_user_key)


async def load_address(
    dataloader: DataLoader, employee_uuid: UUID, address_type_user_key: str
) -> Address | None:
    result = await dataloader.graphql_client.read_filtered_addresses(
        AddressFilter(
            employee=EmployeeFilter(uuids=[employee_uuid]),
            address_type=ClassFilter(user_keys=[address_type_user_key]),
            from_date=None,
            to_date=None,
        )
    )
    address = only(result.objects)
    if address is None:
        logger.info(
            "Could not find employee address",
            employee_uuid=employee_uuid,
            address_type_user_key=address_type_user_key,
        )
        return None
    validity = extract_current_or_latest_validity(address.validities)
    if validity is None:  # pragma: no cover
        logger.error(
            "No active validities on employee address",
            employee_uuid=employee_uuid,
            address_type_user_key=address_type_user_key,
        )
        raise RequeueMessage("No active validities on employee address")
    fetched_address = await dataloader.moapi.load_mo_address(
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
    dataloader: DataLoader, employee_uuid: UUID, address_type_user_key: str
) -> Address | None:
    primary_engagement_uuid = await get_primary_engagement(
        dataloader.graphql_client, EmployeeUUID(employee_uuid)
    )
    if primary_engagement_uuid is None:
        logger.info(
            "Could not find primary engagement UUID", employee_uuid=employee_uuid
        )
        return None

    result = await dataloader.graphql_client.read_filtered_addresses(
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
    fetched_address = await dataloader.moapi.load_mo_address(
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


def skip_if_none(obj: T | None) -> T:
    if obj is None:
        raise SkipObject("Object is None")
    return obj


def construct_globals_dict(
    settings: Settings, dataloader: DataLoader
) -> dict[str, Any]:
    return {
        "now": datetime.utcnow,  # TODO: timezone-aware datetime
        "get_employee_address_type_uuid": partial(
            get_employee_address_type_uuid, dataloader.graphql_client
        ),
        "get_it_system_uuid": partial(dataloader.moapi.get_it_system_uuid),
        "get_visibility_uuid": partial(get_visibility_uuid, dataloader.graphql_client),
        "get_primary_type_uuid": partial(
            get_primary_type_uuid, dataloader.graphql_client
        ),
        "get_engagement_type_uuid": partial(
            get_engagement_type_uuid, dataloader.graphql_client
        ),
        "get_engagement_type_name": partial(
            get_engagement_type_name, dataloader.graphql_client
        ),
        "uuid4": uuid4,
        "get_org_unit_path_string": partial(
            get_org_unit_path_string,
            dataloader.graphql_client,
            settings.org_unit_path_string_separator,
        ),
        "get_org_unit_name_for_parent": partial(
            get_org_unit_name_for_parent, dataloader.graphql_client
        ),
        "make_dn_from_org_unit_path": partial(
            make_dn_from_org_unit_path, settings.org_unit_path_string_separator
        ),
        "get_job_function_name": partial(
            get_job_function_name, dataloader.graphql_client
        ),
        "get_org_unit_name": partial(get_org_unit_name, dataloader.graphql_client),
        "get_or_create_job_function_uuid": partial(
            get_or_create_job_function_uuid, dataloader
        ),
        "get_or_create_engagement_type_uuid": partial(
            get_or_create_engagement_type_uuid, dataloader
        ),
        "get_current_org_unit_uuid_dict": partial(
            get_current_org_unit_uuid_dict, dataloader
        ),
        "get_current_engagement_type_uuid_dict": partial(
            get_current_engagement_type_uuid_dict, dataloader
        ),
        "get_current_primary_uuid_dict": partial(
            get_current_primary_uuid_dict, dataloader
        ),
        "get_primary_engagement_dict": partial(get_primary_engagement_dict, dataloader),
        "get_employee_dict": partial(get_employee_dict, dataloader),
        # These names are intentionally bad, but consistent with the old code names
        # TODO: Rename these functions once the old template system is gone
        "load_mo_employee": dataloader.moapi.load_mo_employee,
        "load_mo_primary_engagement": partial(load_primary_engagement, dataloader),
        "load_mo_it_user": partial(load_it_user, dataloader),
        "load_mo_address": partial(load_address, dataloader),
        "load_mo_org_unit_address": partial(load_org_unit_address, dataloader),
        "create_mo_it_user": partial(create_mo_it_user, dataloader),
        "generate_username": partial(generate_username, dataloader),
        "skip_if_none": skip_if_none,
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


def construct_environment(settings: Settings, dataloader: DataLoader) -> Environment:
    # We intentionally use 'StrictUndefined' here so undefined accesses yield exceptions
    # instead of silently coercing to falsy values as is the case with 'Undefined'
    # See: https://jinja.palletsprojects.com/en/3.1.x/api/#undefined-types
    # For more details.
    environment = Environment(undefined=NeverUndefined, enable_async=True)

    environment.filters["bitwise_and"] = bitwise_and
    environment.filters["splitfirst"] = filter_splitfirst
    environment.filters["splitlast"] = filter_splitlast
    environment.filters["mo_datestring"] = filter_mo_datestring
    environment.filters["strip_non_digits"] = filter_strip_non_digits
    environment.filters["remove_curly_brackets"] = filter_remove_curly_brackets

    environment.globals.update(construct_globals_dict(settings, dataloader))

    return environment
