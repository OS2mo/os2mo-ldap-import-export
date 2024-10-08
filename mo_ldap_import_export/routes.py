# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
"""HTTP Endpoints."""

import re
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import suppress
from datetime import datetime
from functools import partial
from itertools import count
from typing import Any
from uuid import UUID
from uuid import uuid4

import structlog
from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from fastapi import Response
from fastapi import status
from fastapi.encoders import jsonable_encoder
from ldap3 import Connection
from ldap3.protocol import oid
from more_itertools import one
from pydantic import ValidationError
from pydantic import parse_obj_as
from ramodels.mo._shared import validate_cpr

from . import depends
from .autogenerated_graphql_client import GraphQLClient
from .autogenerated_graphql_client.input_types import ITUserFilter
from .autogenerated_graphql_client.input_types import ITUserTerminateInput
from .config import Settings
from .converters import LdapConverter
from .dataloaders import DataLoader
from .exceptions import InvalidCPR
from .ldap import get_attribute_types
from .ldap import get_ldap_attributes
from .ldap import get_ldap_object
from .ldap import get_ldap_schema
from .ldap import get_ldap_superiors
from .ldap import make_ldap_object
from .ldap import paged_search
from .ldap_classes import LdapObject
from .ldap_emit import publish_uuids
from .processors import _hide_cpr as hide_cpr
from .types import DN
from .types import CPRNumber

logger = structlog.stdlib.get_logger()


async def valid_cpr(cpr: str) -> CPRNumber:
    cpr = cpr.replace("-", "")
    if not re.match(r"^\d{10}$", cpr):
        raise InvalidCPR(f"{cpr} is not a valid cpr-number")

    return CPRNumber(cpr)


class CPRFieldNotFound(HTTPException):
    def __init__(self, message):
        super().__init__(status_code=404, detail=message)


class ObjectGUIDITSystemNotFound(HTTPException):
    def __init__(self, message):
        super().__init__(status_code=404, detail=message)


def encode_result(result):
    # This removes all bytes objects from the result. for example images
    json_compatible_result = jsonable_encoder(
        result, custom_encoder={bytes: lambda _: None}
    )
    return json_compatible_result


async def load_ldap_attribute_values(
    settings: Settings, ldap_connection: Connection, attribute, search_base=None
) -> set[str]:
    """
    Returns all values belonging to an LDAP attribute
    """
    searchParameters = {
        "search_filter": "(objectclass=*)",
        "attributes": [attribute],
    }

    responses = await paged_search(
        settings,
        ldap_connection,
        searchParameters,
        search_base=search_base,
    )
    return {str(r["attributes"][attribute]) for r in responses}


async def load_ldap_objects(
    settings: Settings,
    ldap_connection: Connection,
    converter: LdapConverter,
    json_key: str,
    additional_attributes: list[str] | None = None,
    search_base: str | None = None,
) -> list[LdapObject]:
    """
    Returns list with desired ldap objects

    Accepted json_keys are:
        - 'Employee'
        - a MO address type name
    """
    additional_attributes = additional_attributes or []

    user_class = converter.find_ldap_object_class(json_key)
    attributes = converter.get_ldap_attributes(json_key) + additional_attributes

    searchParameters = {
        "search_filter": f"(objectclass={user_class})",
        "attributes": list(set(attributes)),
    }

    responses = await paged_search(
        settings,
        ldap_connection,
        searchParameters,
        search_base=search_base,
    )

    output: list[LdapObject]
    output = [await make_ldap_object(r, ldap_connection, nest=False) for r in responses]

    return output


async def load_ldap_populated_overview(
    settings: Settings,
    ldap_connection: Connection,
    ldap_classes: list[str],
) -> dict:
    """
    Like load_ldap_overview but only returns fields which actually contain data
    """
    nan_values: list[None | list] = [None, []]

    output = {}
    overview = load_ldap_overview(ldap_connection)

    for ldap_class in ldap_classes:
        searchParameters = {
            "search_filter": f"(objectclass={ldap_class})",
            "attributes": ["*"],
        }

        responses = await paged_search(settings, ldap_connection, searchParameters)
        responses = [
            r
            for r in responses
            if r["attributes"]["objectClass"][-1].lower() == ldap_class.lower()
        ]

        populated_attributes = []
        example_value_dict = {}
        for response in responses:
            for attribute, value in response["attributes"].items():
                if value not in nan_values:
                    populated_attributes.append(attribute)
                    if attribute not in example_value_dict:
                        example_value_dict[attribute] = value
        populated_attributes = list(set(populated_attributes))

        if len(populated_attributes) > 0:
            superiors = overview[ldap_class]["superiors"]
            output[ldap_class] = make_overview_entry(
                ldap_connection, populated_attributes, superiors, example_value_dict
            )

    return output


async def paged_query(
    query_func: Callable[[Any], Awaitable[Any]],
) -> AsyncIterator[Any]:
    cursor = None
    for page_counter in count():
        logger.info("Loading next page", page=page_counter)
        result = await query_func(cursor)
        for i in result.objects:
            yield i
        cursor = result.page_info.next_cursor
        if cursor is None:
            return


async def load_all_current_it_users(
    graphql_client: GraphQLClient, it_system_uuid: UUID
) -> list[dict]:
    """
    Loads all current it-users
    """
    filter = parse_obj_as(ITUserFilter, {"itsystem": {"uuids": [it_system_uuid]}})
    read_all_itusers = partial(graphql_client.read_all_itusers, filter)
    return [
        jsonable_encoder(one(entry.validities))
        async for entry in paged_query(read_all_itusers)
        if entry.validities
    ]


async def get_non_existing_unique_ldap_uuids(
    settings: Settings, ldap_connection: Connection, dataloader: DataLoader
) -> list[dict[str, Any]]:
    it_system_uuid = await dataloader.get_ldap_it_system_uuid()
    if not it_system_uuid:
        raise ObjectGUIDITSystemNotFound("Could not find it_system_uuid")

    # Fetch all entity UUIDs in LDAP
    ldap_uuid_attributes = await load_ldap_attribute_values(
        settings, ldap_connection, settings.ldap_unique_id_field
    )
    # load_ldap_attribute_values stringify the attribute values before converting them
    # to a set, thus if one or more entries do not have the attribute, we may end up
    # with the string '[]' in our output. '[]' is not an UUID so we discard it.
    ldap_uuid_attributes.discard("[]")

    unique_ldap_uuids = set(map(UUID, ldap_uuid_attributes))

    # Fetch all MO IT-users and extract all LDAP UUIDs
    all_it_users = await load_all_current_it_users(
        dataloader.graphql_client, UUID(it_system_uuid)
    )
    it_user_map = {UUID(it_user["user_key"]): it_user for it_user in all_it_users}
    unique_ituser_ldap_uuids = set(it_user_map.keys())

    # Find LDAP UUIDs in MO, which do not exist in LDAP
    ituser_uuids_not_in_ldap = unique_ituser_ldap_uuids - unique_ldap_uuids
    return [
        {
            "ituser_uuid": it_user_map[uuid]["uuid"],
            "mo_employee_uuid": it_user_map[uuid]["employee_uuid"],
            "unique_ldap_uuid": it_user_map[uuid]["user_key"],
        }
        for uuid in ituser_uuids_not_in_ldap
    ]


def make_overview_entry(
    ldap_connection: Connection, attributes, superiors, example_value_dict=None
):
    attribute_types = get_attribute_types(ldap_connection)
    attribute_dict = {}
    for attribute in attributes:
        # skip unmapped types
        if attribute not in attribute_types:
            continue
        syntax = attribute_types[attribute].syntax

        # decoded syntax tuple structure: (oid, kind, name, docs)
        syntax_decoded = oid.decode_syntax(syntax)
        details_dict = {
            "syntax": syntax,
        }
        if syntax_decoded:
            details_dict["field_type"] = syntax_decoded[2]

        if example_value_dict and attribute in example_value_dict:
            details_dict["example_value"] = example_value_dict[attribute]

        attribute_dict[attribute] = details_dict

    return {
        "superiors": superiors,
        "attributes": attribute_dict,
    }


def load_ldap_overview(ldap_connection: Connection):
    schema = get_ldap_schema(ldap_connection)

    all_object_classes = sorted(list(schema.object_classes.keys()))

    output = {}
    for ldap_class in all_object_classes:
        all_attributes = get_ldap_attributes(ldap_connection, ldap_class)
        superiors = get_ldap_superiors(ldap_connection, ldap_class)
        output[ldap_class] = make_overview_entry(
            ldap_connection, all_attributes, superiors
        )

    return output


def construct_router(settings: Settings) -> APIRouter:
    router = APIRouter()

    default_ldap_class = settings.conversion_mapping.mo_to_ldap["Employee"].objectClass

    # Load all users from LDAP, and import them into MO
    @router.get("/Import", status_code=202, tags=["Import"])
    async def import_all_objects_from_LDAP(
        settings: depends.Settings,
        ldap_connection: depends.Connection,
        ldap_amqpsystem: depends.LDAPAMQPSystem,
        converter: depends.LdapConverter,
        test_on_first_20_entries: bool = False,
        cpr_indexed_entries_only: bool = True,
        search_base: str | None = None,
    ) -> Any:
        cpr_field = settings.ldap_cpr_attribute

        if cpr_indexed_entries_only and not cpr_field:
            raise CPRFieldNotFound("cpr_field is not configured")

        additional_attributes = [settings.ldap_unique_id_field]

        all_ldap_objects = await load_ldap_objects(
            settings,
            ldap_connection,
            converter,
            "Employee",
            additional_attributes=additional_attributes,
            search_base=search_base,
        )
        number_of_entries = len(all_ldap_objects)
        logger.info("Found entries in LDAP", count=number_of_entries)

        if test_on_first_20_entries:
            # Only upload the first 20 entries
            logger.info("Slicing the first 20 entries")
            all_ldap_objects = all_ldap_objects[:20]

        def has_valid_cpr_number(ldap_object: LdapObject) -> bool:
            assert cpr_field is not None
            cpr_no = CPRNumber(getattr(ldap_object, cpr_field))
            with suppress(ValueError, TypeError):
                validate_cpr(cpr_no)
                return True
            logger.info("Invalid CPR Number found", dn=ldap_object.dn)
            return False

        if cpr_indexed_entries_only:
            all_ldap_objects = list(filter(has_valid_cpr_number, all_ldap_objects))

        uuids = [
            getattr(obj, settings.ldap_unique_id_field) for obj in all_ldap_objects
        ]
        await publish_uuids(ldap_amqpsystem, uuids)

        return uuids

    # Load a single user from LDAP, and import him/her/hir into MO
    @router.get("/Import/{unique_ldap_uuid}", status_code=202, tags=["Import"])
    async def import_single_user_from_LDAP(
        ldap_amqpsystem: depends.LDAPAMQPSystem,
        unique_ldap_uuid: UUID,
        dataloader: depends.DataLoader,
    ) -> Any:
        # Check that we can find the UUID
        await dataloader.ldapapi.get_ldap_dn(unique_ldap_uuid)
        await publish_uuids(ldap_amqpsystem, [unique_ldap_uuid])

    @router.get("/Inspect/dn2uuid/{dn}", status_code=200, tags=["LDAP"])
    async def ldap_dn2uuid(dataloader: depends.DataLoader, dn: str) -> UUID:
        return await dataloader.get_ldap_unique_ldap_uuid(dn)

    @router.get("/Inspect/uuid2dn/{uuid}", status_code=200, tags=["LDAP"])
    async def ldap_uuid2dn(dataloader: depends.DataLoader, uuid: UUID) -> str:
        return await dataloader.ldapapi.get_ldap_dn(uuid)

    @router.get("/Inspect/dn/{dn}", status_code=200, tags=["LDAP"])
    async def ldap_fetch_object_by_dn(
        ldap_connection: depends.Connection, dn: str, nest: bool = False
    ) -> Any:
        return encode_result(
            await get_ldap_object(ldap_connection, dn, ["*"], nest=nest)
        )

    @router.get("/Inspect/uuid/{uuid}", status_code=200, tags=["LDAP"])
    async def ldap_fetch_object_by_uuid(
        dataloader: depends.DataLoader,
        ldap_connection: depends.Connection,
        uuid: UUID,
        nest: bool = False,
    ) -> Any:
        dn = await dataloader.ldapapi.get_ldap_dn(uuid)
        return encode_result(
            await get_ldap_object(ldap_connection, dn, ["*"], nest=nest)
        )

    @router.get("/Inspect/mo2ldap/{uuid}", status_code=200, tags=["LDAP"])
    async def mo2ldap_templating(sync_tool: depends.SyncTool, uuid: UUID) -> Any:
        return encode_result(
            await sync_tool.listen_to_changes_in_employees(uuid, dry_run=True)
        )

    @router.get("/Inspect/mo/uuid2dn/{uuid}", status_code=200, tags=["LDAP"])
    async def mo_uuid_to_ldap_dn(dataloader: depends.DataLoader, uuid: UUID) -> set[DN]:
        return await dataloader.find_mo_employee_dn(uuid)

    # Get all objects from LDAP - Converted to MO
    @router.get("/LDAP/{json_key}/converted", status_code=202, tags=["LDAP"])
    async def convert_all_objects_from_ldap(
        settings: depends.Settings,
        ldap_connection: depends.Connection,
        converter: depends.LdapConverter,
        json_key: str,
    ) -> Any:
        result = await load_ldap_objects(settings, ldap_connection, converter, json_key)
        converted_results = []
        for r in result:
            try:
                converted_results.extend(
                    await converter.from_ldap(r, json_key, employee_uuid=uuid4())
                )
            except ValidationError:
                logger.exception(
                    "Cannot convert LDAP object to MO", ldap_object=r, json_key=json_key
                )
        return converted_results

    # Get a specific cpr-indexed object from LDAP
    @router.get("/LDAP/{json_key}/{cpr}", status_code=202, tags=["LDAP"])
    async def load_object_from_LDAP(
        dataloader: depends.DataLoader,
        settings: depends.Settings,
        json_key: str,
        cpr: CPRNumber = Depends(valid_cpr),
    ) -> Any:
        results = await dataloader.load_ldap_cpr_object(
            cpr, json_key, [settings.ldap_unique_id_field]
        )
        return [encode_result(result) for result in results]

    # Get a specific cpr-indexed object from LDAP - Converted to MO
    @router.get("/LDAP/{json_key}/{cpr}/converted", status_code=202, tags=["LDAP"])
    async def convert_object_from_LDAP(
        dataloader: depends.DataLoader,
        converter: depends.LdapConverter,
        json_key: str,
        response: Response,
        cpr: CPRNumber = Depends(valid_cpr),
    ) -> Any:
        results = await dataloader.load_ldap_cpr_object(cpr, json_key)
        try:
            return [
                await converter.from_ldap(result, json_key, employee_uuid=uuid4())
                for result in results
            ]
        except ValidationError:
            logger.exception(
                "Cannot convert LDAP object to to MO",
                ldap_objects=results,
                json_key=json_key,
            )
            response.status_code = (
                status.HTTP_404_NOT_FOUND
            )  # TODO: return other status?
            return None

    # Get all objects from LDAP
    @router.get("/LDAP/{json_key}", status_code=202, tags=["LDAP"])
    async def load_all_objects_from_LDAP(
        settings: depends.Settings,
        ldap_connection: depends.Connection,
        converter: depends.LdapConverter,
        json_key: str,
        entries_to_return: int = Query(ge=1),
    ) -> Any:
        result = await load_ldap_objects(
            settings,
            ldap_connection,
            converter,
            json_key,
            [settings.ldap_unique_id_field],
        )
        return encode_result(result[-entries_to_return:])

    @router.get(
        "/Inspect/non_existing_unique_ldap_uuids", status_code=202, tags=["LDAP"]
    )
    async def get_non_existing_unique_ldap_uuids_from_MO(
        settings: depends.Settings,
        ldap_connection: depends.Connection,
        dataloader: depends.DataLoader,
    ) -> list[dict[str, Any]]:
        return await get_non_existing_unique_ldap_uuids(
            settings, ldap_connection, dataloader
        )

    @router.post(
        "/fixup/delete_non_existing_unique_ldap_uuids", status_code=200, tags=["LDAP"]
    )
    async def delete_non_existing_unique_ldap_uuids_from_MO(
        settings: depends.Settings,
        ldap_connection: depends.Connection,
        dataloader: depends.DataLoader,
        at: datetime,
    ) -> set[UUID]:
        bad_itusers = await get_non_existing_unique_ldap_uuids(
            settings, ldap_connection, dataloader
        )

        deleted = set()
        for entry in bad_itusers:
            ituser_uuid = entry["ituser_uuid"]
            result = await dataloader.graphql_client.ituser_terminate(
                ITUserTerminateInput(uuid=UUID(ituser_uuid), to=at)
            )
            deleted.add(result.uuid)
        return deleted

    @router.get("/Inspect/duplicate_cpr_numbers", status_code=202, tags=["LDAP"])
    async def get_duplicate_cpr_numbers_from_LDAP(
        settings: depends.Settings,
        ldap_connection: depends.Connection,
    ) -> Any:
        cpr_field = settings.ldap_cpr_attribute
        if not cpr_field:
            raise CPRFieldNotFound("cpr_field is not configured")

        searchParameters = {
            "search_filter": "(objectclass=*)",
            "attributes": [cpr_field],
        }

        responses = [
            r
            for r in await paged_search(settings, ldap_connection, searchParameters)
            if r["attributes"][cpr_field]
        ]

        cpr_values = [r["attributes"][cpr_field] for r in responses]
        output = {}

        for cpr in set(cpr_values):
            if cpr_values.count(cpr) > 1:
                output[hide_cpr(cpr)] = [
                    r["dn"] for r in responses if r["attributes"][cpr_field] == cpr
                ]

        return output

    # Get all objects from LDAP with invalid cpr numbers
    @router.get("/Inspect/invalid_cpr_numbers", status_code=202, tags=["LDAP"])
    async def get_invalid_cpr_numbers_from_LDAP(
        settings: depends.Settings,
        ldap_connection: depends.Connection,
        converter: depends.LdapConverter,
    ) -> Any:
        cpr_field = settings.ldap_cpr_attribute
        if not cpr_field:
            raise CPRFieldNotFound("cpr_field is not configured")

        result = await load_ldap_objects(
            settings, ldap_connection, converter, "Employee"
        )

        formatted_result = {}
        for entry in result:
            cpr = str(getattr(entry, cpr_field))

            try:
                validate_cpr(cpr)
            except ValueError:
                formatted_result[entry.dn] = cpr
        return formatted_result

    # Get LDAP overview
    @router.get("/Inspect/overview", status_code=202, tags=["LDAP"])
    async def load_overview_from_LDAP(
        ldap_connection: depends.Connection,
        ldap_class: str = default_ldap_class,
    ) -> Any:
        ldap_overview = load_ldap_overview(ldap_connection)
        return ldap_overview[ldap_class]

    # Get LDAP overview
    @router.get("/Inspect/structure", status_code=202, tags=["LDAP"])
    async def load_structure_from_LDAP(
        dataloader: depends.DataLoader, search_base: str | None = None
    ) -> Any:
        return await dataloader.load_ldap_OUs(search_base=search_base)

    # Get populated LDAP overview
    @router.get("/Inspect/overview/populated", status_code=202, tags=["LDAP"])
    async def load_populated_overview_from_LDAP(
        settings: depends.Settings,
        ldap_connection: depends.Connection,
        ldap_class: str = default_ldap_class,
    ) -> Any:
        ldap_overview = await load_ldap_populated_overview(
            settings, ldap_connection, ldap_classes=[ldap_class]
        )
        return encode_result(ldap_overview.get(ldap_class))

    # Get LDAP attribute details
    @router.get("/Inspect/attribute/{attribute}", status_code=202, tags=["LDAP"])
    async def load_attribute_details_from_LDAP(
        ldap_connection: depends.Connection,
        attribute: str,
    ) -> Any:
        # TODO: This is already available in the construct_router scope
        #       Should we just access that, or is the core issue that it is cached?
        #       I.e. Should we just accept any attribute str, not just the ones that
        #       we find on program startup?
        attribute_types = get_attribute_types(ldap_connection)
        return attribute_types[attribute]

    # Get LDAP attribute values
    @router.get("/Inspect/attribute/values/{attribute}", status_code=202, tags=["LDAP"])
    async def load_unique_attribute_values_from_LDAP(
        settings: depends.Settings,
        ldap_connection: depends.Connection,
        attribute: str,
        search_base: str | None = None,
    ) -> Any:
        return sorted(
            await load_ldap_attribute_values(
                settings, ldap_connection, attribute, search_base=search_base
            )
        )

    return router
