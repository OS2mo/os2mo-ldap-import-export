# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
"""Dataloaders to bulk requests."""
import asyncio
import datetime
from enum import auto
from enum import Enum
from typing import Any
from typing import cast
from uuid import UUID

from fastapi.encoders import jsonable_encoder
from gql import gql
from gql.client import AsyncClientSession
from gql.transport.exceptions import TransportQueryError
from graphql import DocumentNode
from ldap3 import BASE
from ldap3.core.exceptions import LDAPInvalidValueError
from ldap3.protocol import oid
from ldap3.utils.dn import safe_dn
from ldap3.utils.dn import to_dn
from more_itertools import only
from more_itertools import partition
from ramodels.mo import MOBase
from ramodels.mo._shared import EngagementRef
from ramodels.mo._shared import validate_cpr
from ramodels.mo.details.address import Address
from ramodels.mo.details.engagement import Engagement
from ramodels.mo.details.it_system import ITUser
from ramodels.mo.employee import Employee

from .environments import filter_remove_curly_brackets
from .exceptions import AttributeNotFound
from .exceptions import DNNotFound
from .exceptions import InvalidChangeDict
from .exceptions import InvalidQueryResponse
from .exceptions import MultipleObjectsReturnedException
from .exceptions import NoObjectsReturnedException
from .exceptions import NotEnabledException
from .exceptions import UUIDNotFoundException
from .ldap import get_attribute_types
from .ldap import get_ldap_attributes
from .ldap import get_ldap_schema
from .ldap import get_ldap_superiors
from .ldap import is_uuid
from .ldap import make_ldap_object
from .ldap import paged_search
from .ldap import single_object_search
from .ldap_classes import LdapObject
from .logging import logger
from .processors import _hide_cpr as hide_cpr
from .utils import add_filter_to_query
from .utils import combine_dn_strings
from .utils import extract_cn_from_dn
from .utils import extract_ou_from_dn
from .utils import mo_datestring_to_utc
from .utils import remove_cn_from_dn

DNList = list[str]


class Verb(Enum):
    CREATE = auto()
    EDIT = auto()


class DataLoader:
    def __init__(self, context):
        self.context = context
        self.user_context = context["user_context"]
        self.ldap_connection = self.user_context["ldap_connection"]
        self.attribute_types = get_attribute_types(self.ldap_connection)
        self.single_value = {
            a: self.attribute_types[a].single_value for a in self.attribute_types.keys()
        }
        self._mo_to_ldap_attributes = []
        self._sync_tool = None
        self.create_mo_class_lock = asyncio.Lock()

        # Relate graphQL object types (left) to AMQP routing key object types (right)
        self.object_type_dict = {
            "employees": "person",
            "org_units": "org_unit",
            "addresses": "address",
            "itusers": "ituser",
            "engagements": "engagement",
        }

        self.object_type_dict_inv = {
            str(v): k for k, v in self.object_type_dict.items()
        }

        self.supported_object_types = list(self.object_type_dict_inv.keys())

    def _check_if_empty(self, result: dict):
        for key, value in result.items():
            if "objects" in value and len(value["objects"]) == 0:
                raise NoObjectsReturnedException(
                    f"query_result['{key}'] is empty. "
                    f"Does the '{key}' object still exist as a current object? "
                    f"Does the '{key}' object exist in MO?"
                )

    @property
    def sync_tool(self):
        if not self._sync_tool:
            self._sync_tool = self.user_context["sync_tool"]
        return self._sync_tool

    @property
    def mo_to_ldap_attributes(self):
        """
        Populates self._mo_to_ldap_attributes and returns it.

        self._mo_to_ldap_attributes is a list of all LDAP attribute names which
        are synchronized to LDAP

        Notes
        -------
        This is not done in __init__() because the converter is not initialized yet,
        when we initialize the dataloader.
        """
        if not self._mo_to_ldap_attributes:
            converter = self.user_context["converter"]
            for json_dict in converter.mapping["mo_to_ldap"].values():
                self._mo_to_ldap_attributes.extend(list(json_dict.keys()))
        return self._mo_to_ldap_attributes

    def shared_attribute(self, attribute: str):
        """
        Determine if an attribute is shared between multiple LDAP objects.

        Parameters
        ------------
        attribute : str
            LDAP attribute name

        Returns
        ----------
        return_value : bool
            True if the attribute is shared between different LDAP objects, False if it
            is not.

        Examples
        -----------
        >>> self.shared_attribute("cpr_no")
        >>> True

        The "cpr_no" attribute is generally shared between different LDAP objects.
        Therefore the return value is "True"

        >>> self.shared_attribute("mobile_phone_no")
        >>> False

        An attribute which contains a phone number is generally only used by a single
        LDAP object. Therefore the return value is "False"

        Notes
        -------
        The return value in above examples depends on the json dictionary.
        """
        occurences = self.mo_to_ldap_attributes.count(attribute)
        if occurences == 1:
            return False
        elif occurences > 1:
            return True
        else:
            raise AttributeNotFound(
                f"'{attribute}' not found in 'mo_to_ldap' attributes"
            )

    async def query_mo(
        self, query: DocumentNode, raise_if_empty: bool = True, variable_values={}
    ):
        graphql_session: AsyncClientSession = self.user_context["gql_client"]
        result = await graphql_session.execute(
            query, variable_values=jsonable_encoder(variable_values)
        )
        if raise_if_empty:
            self._check_if_empty(result)
        return result

    async def query_mo_paged(self, query):
        result = await self.query_mo(query, raise_if_empty=False)

        for key in result.keys():
            cursor = result[key]["page_info"]["next_cursor"]
            page_counter = 0

            while cursor:
                logger.info(f"[Paged-query] Loading {key} - page {page_counter}")
                next_result = await self.query_mo(
                    query,
                    raise_if_empty=False,
                    variable_values={"cursor": cursor},
                )

                # Append next page to result
                result[key]["objects"] += next_result[key]["objects"]

                # Update cursor and page counter
                page_counter += 1
                cursor = next_result[key]["page_info"]["next_cursor"]

        return result

    async def query_past_future_mo(
        self, query: DocumentNode, current_objects_only: bool
    ):
        """
        First queries MO. If no objects are returned, attempts to query past/future
        objects as well
        """
        try:
            return await self.query_mo(query)
        except NoObjectsReturnedException as e:
            if current_objects_only:
                raise e
            else:
                query = add_filter_to_query(query, "to_date: null, from_date: null")
                return await self.query_mo(query)

    def load_ldap_object(self, dn, attributes, nest=True):
        searchParameters = {
            "search_base": dn,
            "search_filter": "(objectclass=*)",
            "attributes": attributes,
            "search_scope": BASE,
        }
        search_result = single_object_search(searchParameters, self.context)
        return make_ldap_object(search_result, self.context, nest=nest)

    def load_ldap_attribute_values(self, attribute, search_base=None) -> list[str]:
        """
        Returns all values belonging to an LDAP attribute
        """
        searchParameters = {
            "search_filter": "(objectclass=*)",
            "attributes": [attribute],
        }

        responses = paged_search(
            self.context,
            searchParameters,
            search_base=search_base,
        )
        return sorted({str(r["attributes"][attribute]) for r in responses})

    def load_ldap_cpr_object(
        self,
        cpr_no: str,
        json_key: str,
        additional_attributes: list[str] = [],
    ) -> LdapObject:
        """
        Loads an ldap object which can be found using a cpr number lookup

        Accepted json_keys are:
            - 'Employee'
            - a MO address type name
        """
        try:
            validate_cpr(cpr_no)
        except (ValueError, TypeError):
            raise NoObjectsReturnedException(f"cpr_no '{cpr_no}' is invalid")

        cpr_field = self.user_context["cpr_field"]
        if not cpr_field:
            raise NoObjectsReturnedException("cpr_field is not configured")

        settings = self.user_context["settings"]

        search_base = settings.ldap_search_base
        ous_to_search_in = settings.ldap_ous_to_search_in
        search_bases = [
            combine_dn_strings([ou, search_base]) for ou in ous_to_search_in
        ]
        converter = self.user_context["converter"]

        object_class = converter.find_ldap_object_class(json_key)
        attributes = converter.get_ldap_attributes(json_key) + additional_attributes

        object_class_filter = f"objectclass={object_class}"
        cpr_filter = f"{cpr_field}={cpr_no}"

        searchParameters = {
            "search_base": search_bases,
            "search_filter": f"(&({object_class_filter})({cpr_filter}))",
            "attributes": list(set(attributes)),
        }
        search_result = single_object_search(searchParameters, self.context)

        ldap_object: LdapObject = make_ldap_object(search_result, self.context)
        logger.info("[Load-ldap-cpr-object] Found LDAP object.", dn=ldap_object.dn)

        return ldap_object

    def ou_in_ous_to_write_to(
        self,
        dn: str,
        tag: str = "[OU-in-OUs-to-write-to]",
    ) -> bool:
        """
        Determine if an OU is among those to which we are allowed to write.
        """
        settings = self.user_context["settings"]

        if "" in settings.ldap_ous_to_write_to:
            # Empty string means that it is allowed to write to all OUs
            return True

        ou = extract_ou_from_dn(dn)
        ous_to_write_to = [safe_dn(ou) for ou in settings.ldap_ous_to_write_to]
        for ou_to_write_to in ous_to_write_to:
            if ou.endswith(ou_to_write_to):
                # If an OU ends with one of the OUs-to-write-to, it's OK.
                # For example, if we are only allowed to write to "OU=foo",
                # Then we are also allowed to write to "OU=bar,OU=foo", which is a
                # sub-OU inside "OU=foo"
                return True

        logger.info(f"{tag} {ou} is not in {ous_to_write_to}")
        return False

    def modify_ldap(
        self,
        dn: str,
        changes: (
            dict[str, list[tuple[str, list[str]]]] | dict[str, list[tuple[str, str]]]
        ),
    ):
        """
        Modifies LDAP and adds the dn to dns_to_ignore
        """
        # Checks
        if not self.ou_in_ous_to_write_to(dn, "[Modify-ldap]"):
            return

        attributes = list(changes.keys())
        if len(attributes) != 1:
            raise InvalidChangeDict("Exactly one attribute can be changed at a time")

        attribute = attributes[0]
        list_of_changes = changes[attribute]
        if len(list_of_changes) != 1:
            raise InvalidChangeDict("Exactly one change can be submitted at a time")

        ldap_command, value_to_modify = list_of_changes[0]
        if type(value_to_modify) is list:
            if len(value_to_modify) == 1:
                value_to_modify = value_to_modify[0]
            elif len(value_to_modify) == 0:
                value_to_modify = ""
            else:
                raise InvalidChangeDict("Exactly one value can be changed at a time")

        # Compare to LDAP
        value_exists = self.ldap_connection.compare(dn, attribute, value_to_modify)

        # Modify LDAP
        if not value_exists or "DELETE" in ldap_command:
            logger.info(f"[Modify-ldap] Uploading the following changes: {changes}")
            self.ldap_connection.modify(dn, changes)
            response = self.log_ldap_response("[Modify-ldap]", dn=dn)

            # If successful, the importer should ignore this DN
            if response["description"] == "success":
                # Clean all old entries
                self.sync_tool.dns_to_ignore.clean()

                # Only add if nothing is there yet. Otherwise we risk adding an
                # ignore-command for every modified parameter
                #
                # Also: even if an LDAP attribute gets modified by us twice within a
                # couple of seconds, it should still only be ignored once; Because we
                # only retrieve the latest state of the LDAP object when polling
                if not self.sync_tool.dns_to_ignore[dn]:
                    self.sync_tool.dns_to_ignore.add(dn)

            return response
        else:
            logger.info(
                f"[Modify-ldap] {attribute}['{value_to_modify}'] already exists"
            )

    def cleanup_attributes_in_ldap(self, ldap_objects: list[LdapObject]):
        """
        Deletes the values belonging to the attributes in the given ldap objects.

        Notes
        ----------
        Will not delete values belonging to attributes which are shared between multiple
        ldap objects. Because deleting an LDAP object should not remove the possibility
        to compile an LDAP object of a different type.
        """
        for ldap_object in ldap_objects:
            logger.info(
                "[Cleanup-attributes-in-LDAP] Processing ldap object.",
                dn=ldap_object.dn,
            )
            attributes_to_clean = [
                a
                for a in ldap_object.dict().keys()
                if a != "dn" and not self.shared_attribute(a)
            ]

            if not attributes_to_clean:
                logger.info("[Cleanup-attributes-in-LDAP] No cleanable attributes.")
                return

            dn = ldap_object.dn
            for attribute in attributes_to_clean:
                value_to_delete = ldap_object.dict()[attribute]
                logger.info(
                    "[Cleanup-attributes-in-LDAP] Cleaning.",
                    value_to_delete=value_to_delete,
                    attribute=attribute,
                )

                changes = {attribute: [("MODIFY_DELETE", value_to_delete)]}
                self.modify_ldap(dn, changes)

    async def load_ldap_objects(
        self,
        json_key: str,
        additional_attributes: list[str] = [],
        search_base: str | None = None,
    ) -> list[LdapObject]:
        """
        Returns list with desired ldap objects

        Accepted json_keys are:
            - 'Employee'
            - a MO address type name
        """
        converter = self.user_context["converter"]
        user_class = converter.find_ldap_object_class(json_key)
        attributes = converter.get_ldap_attributes(json_key) + additional_attributes

        searchParameters = {
            "search_filter": f"(objectclass={user_class})",
            "attributes": list(set(attributes)),
        }

        responses = paged_search(
            self.context,
            searchParameters,
            search_base=search_base,
        )

        output: list[LdapObject]
        output = [make_ldap_object(r, self.context, nest=False) for r in responses]

        return output

    def load_ldap_OUs(self, search_base: str | None = None) -> dict:
        """
        Returns a dictionary where the keys are OU strings and the items are dicts
        which contain information about the OU
        """
        searchParameters: dict = {
            "search_filter": "(objectclass=OrganizationalUnit)",
            "attributes": [],
        }

        responses = paged_search(
            self.context,
            searchParameters,
            search_base=search_base,
            mute=True,
        )

        dns = [r["dn"] for r in responses]
        output = {}

        for dn in dns:
            searchParameters = {
                "search_filter": "(objectclass=user)",
                "attributes": [],
                "size_limit": 1,
            }

            responses = paged_search(
                self.context,
                searchParameters,
                search_base=dn,
                mute=True,
            )
            ou = extract_ou_from_dn(dn)
            if len(responses) == 0:
                output[ou] = {"empty": True}
            else:
                output[ou] = {"empty": False}
            output[ou]["dn"] = dn

        return output

    def log_ldap_response(self, tag, **kwargs) -> dict:
        response: dict = self.ldap_connection.result
        logger.info(f"{tag} Response:", response=response, **kwargs)
        return response

    def add_ldap_object(self, dn: str, attributes: dict[str, Any] | None = None):
        """
        Adds a new object to LDAP

        Parameters
        ---------------
        attributes : dict
            dictionary with attributes to populate in LDAP, when creating the user.
            See https://ldap3.readthedocs.io/en/latest/add.html for more information

        """
        settings = self.user_context["settings"]
        if not settings.add_objects_to_ldap:
            logger.info("[Add-ldap-object] add_objects_to_ldap = False. Aborting.")
            raise NotEnabledException("Adding LDAP objects is disabled")

        if not self.ou_in_ous_to_write_to(dn, "[Add-ldap-object]"):
            return

        logger.info(
            "[Add-ldap-object] Adding user to LDAP.", dn=dn, attributes=attributes
        )
        self.ldap_connection.add(
            dn,
            self.user_context["converter"].find_ldap_object_class("Employee"),
            attributes=attributes,
        )
        self.log_ldap_response("[Add-ldap-object]", dn=dn)

    @staticmethod
    def decompose_ou_string(ou: str) -> list[str]:
        """
        Decomposes an OU string and returns a list of OUs where the first one is the
        given OU string, and the last one if the highest parent OU

        Example
        -----------
        >>> ou = 'OU=foo,OU=bar'
        >>> decompose_ou_string(ou)
        >>> ['OU=foo,OU=bar', 'OU=bar']
        """

        ou_parts = to_dn(ou)
        output = []
        for i in range(len(ou_parts)):
            output.append(combine_dn_strings(ou_parts[i:]))

        return output

    def create_ou(self, ou: str) -> None:
        """
        Creates an OU. If the parent OU does not exist, creates that one first
        """
        settings = self.user_context["settings"]
        if not settings.add_objects_to_ldap:
            logger.info("[Create-OU] add_objects_to_ldap = False. Aborting.")
            raise NotEnabledException("Adding LDAP objects is disabled")
        if not self.ou_in_ous_to_write_to(ou, "[Create-OU]"):
            return

        ou_dict = self.load_ldap_OUs()

        # Create OUs top-down (unless they already exist)
        for ou_to_create in self.decompose_ou_string(ou)[::-1]:
            if ou_to_create not in ou_dict:
                logger.info("[Create-OU] Creating OU.", ou_to_create=ou_to_create)
                dn = combine_dn_strings([ou_to_create, settings.ldap_search_base])

                self.ldap_connection.add(dn, "OrganizationalUnit")
                self.log_ldap_response("[Create-OU]", dn=dn)

    def delete_ou(self, ou: str) -> None:
        """
        Deletes an OU. If the parent OU is empty after deleting, also deletes that one

        Notes
        --------
        Only deletes OUs which are empty
        """
        settings = self.user_context["settings"]
        if not self.ou_in_ous_to_write_to(ou, "[Delete-OU]"):
            return

        for ou_to_delete in self.decompose_ou_string(ou):
            ou_dict = self.load_ldap_OUs()
            if (
                ou_dict.get(ou_to_delete, {}).get("empty", False)
                and ou_to_delete != settings.ldap_ou_for_new_users
            ):
                logger.info("[Delete-OU] Deleting OU.", ou_to_delete=ou_to_delete)
                dn = combine_dn_strings([ou_to_delete, settings.ldap_search_base])
                self.ldap_connection.delete(dn)
                self.log_ldap_response("[Delete-OU]", dn=dn)

    def move_ldap_object(self, old_dn: str, new_dn: str) -> bool:
        """
        Moves an LDAP object from one DN to another. Returns True if the move was
        successful.
        """
        settings = self.user_context["settings"]
        if not self.ou_in_ous_to_write_to(new_dn, "[Move-LDAP-object]"):
            return False
        if not settings.add_objects_to_ldap:
            logger.info("[Move-LDAP-object] add_objects_to_ldap = False. Aborting.")
            raise NotEnabledException("Moving LDAP objects is disabled")

        logger.info("[Move-LDAP-object] Moving entry.", old_dn=old_dn, new_dn=new_dn)

        self.ldap_connection.modify_dn(
            old_dn, extract_cn_from_dn(new_dn), new_superior=remove_cn_from_dn(new_dn)
        )

        response = self.log_ldap_response(
            "[Move-LDAP-object]", new_dn=new_dn, old_dn=old_dn
        )
        return True if response["description"] == "success" else False

    async def modify_ldap_object(
        self,
        object_to_modify: LdapObject,
        json_key: str,
        overwrite: bool = False,
        delete: bool = False,
    ) -> list[dict]:
        """
        Parameters
        -------------
        object_to_modify : LDAPObject
            object to upload to LDAP
        json_key : str
            json key to upload. e.g. 'Employee' or 'Engagement' or another key present
            in the json dictionary.
        overwrite: bool
            Set to True to overwrite contents in LDAP
        delete: bool
            Set to True to delete contents in LDAP, instead of creating/modifying them
        """
        converter = self.user_context["converter"]
        if not converter._export_to_ldap_(json_key):
            logger.info(
                "[Modify-ldap-object] _export_to_ldap_ == False.", json_key=json_key
            )
            return []
        success = 0
        failed = 0

        parameters_to_modify = list(object_to_modify.dict().keys())

        logger.info(f"[Modify-ldap-object] Uploading {object_to_modify}.")
        parameters_to_modify = [p for p in parameters_to_modify if p != "dn"]
        dn = object_to_modify.dn
        results = []

        if delete:
            # Only delete parameters which are not shared between different objects.
            # For example: 'org-unit name' should not be deleted if both
            # engagements and org unit addresses use it;
            #
            # If we would delete 'org-unit name' as a part of an org-unit address delete
            # operation, We would suddenly not be able to import engagements any more.
            parameters_to_modify = [
                p for p in parameters_to_modify if not self.shared_attribute(p)
            ]

        for parameter_to_modify in parameters_to_modify:
            value = getattr(object_to_modify, parameter_to_modify)
            value_to_modify: list[str] = [] if value is None else [value]

            if delete:
                changes = {parameter_to_modify: [("MODIFY_DELETE", value_to_modify)]}
            elif self.single_value[parameter_to_modify] or overwrite:
                changes = {parameter_to_modify: [("MODIFY_REPLACE", value_to_modify)]}
            else:
                changes = {parameter_to_modify: [("MODIFY_ADD", value_to_modify)]}

            try:
                response = self.modify_ldap(dn, changes)
            except LDAPInvalidValueError as e:
                logger.warning("[Modify-ldap-object] " + str(e))
                failed += 1
                continue

            if response and response["description"] == "success":
                success += 1
            elif response:
                failed += 1

            if response:
                results.append(response)

        logger.info(
            "[Modify-ldap-object] Succeeded/failed MODIFY_* operations:",
            success=success,
            failed=failed,
        )

        return results

    def make_overview_entry(self, attributes, superiors, example_value_dict=None):

        attribute_dict = {}
        for attribute in attributes:
            # skip unmapped types
            if attribute not in self.attribute_types:
                continue
            syntax = self.attribute_types[attribute].syntax

            # decoded syntax tuple structure: (oid, kind, name, docs)
            syntax_decoded = oid.decode_syntax(syntax)
            details_dict = {
                "single_value": self.attribute_types[attribute].single_value,
                "syntax": syntax,
            }
            if syntax_decoded:
                details_dict["field_type"] = syntax_decoded[2]

            if example_value_dict:
                if attribute in example_value_dict:
                    details_dict["example_value"] = example_value_dict[attribute]

            attribute_dict[attribute] = details_dict

        return {
            "superiors": superiors,
            "attributes": attribute_dict,
        }

    def load_ldap_overview(self):
        schema = get_ldap_schema(self.ldap_connection)

        all_object_classes = sorted(list(schema.object_classes.keys()))

        output = {}
        for ldap_class in all_object_classes:
            all_attributes = get_ldap_attributes(self.ldap_connection, ldap_class)
            superiors = get_ldap_superiors(self.ldap_connection, ldap_class)
            output[ldap_class] = self.make_overview_entry(all_attributes, superiors)

        return output

    def load_ldap_populated_overview(self, ldap_classes=None):
        """
        Like load_ldap_overview but only returns fields which actually contain data
        """
        nan_values: list[None | list] = [None, []]

        output = {}
        overview = self.load_ldap_overview()

        if not ldap_classes:
            ldap_classes = overview.keys()

        for ldap_class in ldap_classes:
            searchParameters = {
                "search_filter": f"(objectclass={ldap_class})",
                "attributes": ["*"],
            }

            responses = paged_search(self.context, searchParameters)
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
                output[ldap_class] = self.make_overview_entry(
                    populated_attributes, superiors, example_value_dict
                )

        return output

    def _return_mo_employee_uuid_result(self, result: dict) -> None | UUID:
        number_of_employees = len(result.get("employees", {}).get("objects", []))
        number_of_itusers = len(result["itusers"]["objects"])
        error_message = hide_cpr(f"Multiple matching employees in {result}")
        exception = MultipleObjectsReturnedException(error_message)

        if number_of_employees == 1:
            logger.info("[Return-mo-employee-uuid] Attempting cpr_no lookup.")
            uuid: UUID = result["employees"]["objects"][0]["uuid"]
            return uuid

        elif number_of_itusers >= 1:
            logger.info("[Return-mo-employee-uuid] Attempting it-system lookup.")
            uuids = [
                result["itusers"]["objects"][i]["objects"][0]["employee_uuid"]
                for i in range(number_of_itusers)
            ]

            return only(set(uuids), too_long=exception)

        elif number_of_itusers == 0 and number_of_employees == 0:
            logger.info(f"[Return-mo-employee-uuid] No matching employee in {result}")
            return None
        else:
            raise exception

    async def find_mo_employee_uuid(self, dn: str) -> None | UUID:
        cpr_field = self.user_context["cpr_field"]
        if cpr_field:
            ldap_object = self.load_ldap_object(dn, [cpr_field])

            # Try to get the cpr number from LDAP and use that.
            try:
                cpr_no = validate_cpr(str(getattr(ldap_object, cpr_field)))
            except ValueError:
                cpr_no = None

        if cpr_field and cpr_no:
            cpr_query = f"""
            employees(cpr_numbers: "{cpr_no}") {{
              objects {{
                 uuid
              }}
            }}
            """
        else:
            cpr_query = ""

        objectGUID = self.get_ldap_objectGUID(dn)
        ituser_query = f"""
        itusers(user_keys: "{objectGUID}") {{
          objects {{
            objects {{
               employee_uuid
            }}
          }}
        }}
        """

        query = gql(
            f"""
            query FindEmployeeUUID {{
              {cpr_query}
              {ituser_query}
            }}
            """
        )

        result = await self.query_mo(query, raise_if_empty=False)
        return self._return_mo_employee_uuid_result(result)

    async def find_mo_engagement_uuid(self, dn: str) -> None | UUID:
        # Get ObjectGUID from DN, then get engagement by looking for IT user with that
        # ObjectGUID in MO.

        ldap_object = self.load_ldap_object(dn, ["objectGUID"])
        object_guid = filter_remove_curly_brackets(ldap_object.objectGUID)

        query = gql(
            """
            query FindEngagementUUID($objectGUID: String!) {
              itusers(user_keys: [$objectGUID]) {
                objects {
                  current {
                    engagement { uuid }
                    itsystem { uuid }
                  }
                }
              }
            }
            """
        )

        result = await self.query_mo(
            query,
            variable_values={  # type: ignore
                "objectGUID": object_guid,
            },
            raise_if_empty=False,
        )

        for it_user in result["itusers"]["objects"]:
            obj = it_user["current"]
            if obj["itsystem"]["uuid"] == self.get_ldap_it_system_uuid():
                if obj["engagement"] is not None and len(obj["engagement"]) > 0:
                    engagement_uuid = UUID(obj["engagement"][0]["uuid"])
                    logger.info(
                        "[Find-mo-engagement-uuid] Found engagement UUID for DN",
                        dn=dn,
                        object_guid=object_guid,
                        engagement_uuid=engagement_uuid,
                    )
                    return engagement_uuid

        logger.info(
            "[Find-mo-engagement-uuid] Could not find engagement UUID for DN",
            dn=dn,
            object_guid=object_guid,
            objects=result["itusers"]["objects"],
        )
        return None

    def get_ldap_it_system_uuid(self) -> str | None:
        """
        Return the IT system uuid belonging to the LDAP-it-system
        Return None if the LDAP-it-system is not found.
        """
        converter = self.user_context["converter"]
        user_key = self.user_context["ldap_it_system_user_key"]
        try:
            return cast(str, converter.get_it_system_uuid(user_key))
        except UUIDNotFoundException:
            logger.info(
                "[Get-ldap-it-system-uuid] UUID Not found.",
                suggestion=f"Does the '{user_key}' it-system exist?",
            )
            return None

    def get_ldap_dn(self, objectGUID: UUID) -> str:
        """
        Given an objectGUID, find the DistinguishedName
        """
        logger.info("[Get-ldap-dn] Looking for LDAP object.", objectGUID=objectGUID)
        searchParameters = {
            "search_base": f"<GUID={objectGUID}>",
            "search_filter": "(objectclass=*)",
            "attributes": [],
            "search_scope": BASE,
        }

        search_result = single_object_search(searchParameters, self.context)
        dn: str = search_result["dn"]
        return dn

    def get_ldap_objectGUID(self, dn: str) -> UUID:
        """
        Given a DN, find the objectGUID
        """
        logger.info("[Get-ldap-objectGUID] Looking for LDAP object.", dn=dn)
        ldap_object = self.load_ldap_object(dn, ["objectGUID"])
        return UUID(ldap_object.objectGUID)

    def extract_unique_objectGUIDs(self, it_users: list[ITUser]) -> set[UUID]:
        """
        Extracts unique objectGUIDs from a list of it-users
        """
        objectGUIDs: list[UUID] = []
        for it_user in it_users:
            user_key = it_user.user_key
            if is_uuid(user_key):
                objectGUIDs.append(UUID(user_key))
            else:
                logger.info(
                    "[Extract-unique-objectGUIDs] it-user is not an objectGUID",
                    user_key=user_key,
                )

        return set(objectGUIDs)

    def extract_unique_dns(self, it_users: list[ITUser]) -> list[str]:
        objectGUIDs = self.extract_unique_objectGUIDs(it_users)
        return [self.get_ldap_dn(objectGUID) for objectGUID in objectGUIDs]

    async def find_or_make_mo_employee_dn(self, uuid: UUID) -> DNList:
        """
        Tries to find the LDAP DN belonging to a MO employee UUID. If such a DN does not
        exist, generates a new one and returns that.

        Parameters
        -------------
        uuid: UUID
            UUID of the employee to generate a DN for

        Notes
        --------
        If a DN could not be found or generated, raises a DNNotFound exception
        """
        logger.info(
            "[Find-or-make-employee-dn] Attempting to find DN.",
            employee_uuid=uuid,
        )
        username_generator = self.user_context["username_generator"]
        raw_it_system_uuid: str | None = self.get_ldap_it_system_uuid()
        if raw_it_system_uuid is not None:
            it_system_uuid: UUID = UUID(raw_it_system_uuid)

        # The LDAP-it-system only exists, if it was configured as such in OS2mo-init.
        # It is not strictly needed; If we purely rely on cpr-lookup we can live
        # without it
        ldap_it_system_exists = True if raw_it_system_uuid else False

        if ldap_it_system_exists:
            it_users = await self.load_mo_employee_it_users(uuid, it_system_uuid)
            dns = self.extract_unique_dns(it_users)
            if dns:
                # If we have an it-user (with a valid dn), use that dn
                logger.info(
                    "[Find-or-make-employee-dn] Found DN(s) using it-user lookup",
                    dns=dns,
                    employee_uuid=uuid,
                )
                return dns

        # If the employee has a cpr-no, try using that to find a matching dn
        employee = await self.load_mo_employee(uuid)
        cpr_no = employee.cpr_no
        if cpr_no:
            logger.info(
                "[Find-or-make-employee-dn] Attempting cpr-lookup.",
                cpr_no=cpr_no,
                employee_uuid=uuid,
            )
            try:
                dn = self.load_ldap_cpr_object(cpr_no, "Employee").dn
                logger.info(
                    "[Find-or-make-employee-dn] Found DN using cpr-lookup.",
                    dn=dn,
                    employee_uuid=uuid,
                    cpr_no=cpr_no,
                )
                return [dn]
            except NoObjectsReturnedException:
                if not ldap_it_system_exists:
                    # If the LDAP-it-system is not configured, we can just generate the
                    # DN and return it. If there is one, we pretty much do the same,
                    # but also need to store the DN in an it-user object.
                    # This is done below.
                    logger.info(
                        "[Find-or-make-employee-dn] LDAP it-system not found.",
                        task="Generating DN",
                        employee_uuid=uuid,
                    )
                    dn = await username_generator.generate_dn(employee)
                    await self.sync_tool.import_single_user(
                        dn, force=True, manual_import=True
                    )
                    await self.sync_tool.refresh_employee(employee.uuid)
                    return [dn]

        # If there are no LDAP-it-users with valid dns, we generate a dn and create one.
        if ldap_it_system_exists and len(dns) == 0:
            logger.info(
                "[Find-or-make-employee-dn] No it-user found.",
                task="Generating DN and creating it-user",
                employee_uuid=uuid,
            )
            dn = await username_generator.generate_dn(employee)

            # Get it's objectGUID
            objectGUID = self.get_ldap_objectGUID(dn)

            # Make a new it-user
            it_user = ITUser.from_simplified_fields(
                str(objectGUID),
                it_system_uuid,
                datetime.datetime.today().strftime("%Y-%m-%d"),
                person_uuid=uuid,
            )
            await self.upload_mo_objects([it_user])
            await self.sync_tool.import_single_user(dn, force=True, manual_import=True)
            await self.sync_tool.refresh_employee(employee.uuid)
            return [dn]
        # If the LDAP-it-system is not configured and the user also does not have a cpr-
        # Number we can end up here.
        else:
            raise DNNotFound(
                f"Could not find or generate DN for empoyee with uuid = '{uuid}' "
                "The LDAP it-system does not exist and a cpr-match could "
                "also not be obtained"
            )

    async def find_dn_by_engagement_uuid(
        self,
        employee_uuid: UUID,
        engagement: EngagementRef,
        dns: DNList,
    ) -> str:
        if len(dns) == 1:
            return dns[0]
        engagement_uuid: UUID | None = getattr(engagement, "uuid", None)
        ldap_it_system_uuid: UUID = UUID(self.get_ldap_it_system_uuid())

        it_users: list[ITUser] = await self.load_mo_employee_it_users(
            employee_uuid,
            ldap_it_system_uuid,
        )
        matching_it_users: list[ITUser] = [
            it_user
            for it_user in it_users
            if (engagement_uuid is None and it_user.engagement is None)
            or (
                engagement_uuid is not None
                and getattr(it_user.engagement, "uuid", None) == engagement_uuid
            )
        ]

        if len(matching_it_users) == 1:
            # Single match, ObjectGUID is stored in ITUser.user_key
            object_guid: UUID = UUID(matching_it_users[0].user_key)
            dn: str = self.get_ldap_dn(object_guid)
            assert dn in dns
            return dn
        elif len(matching_it_users) > 1:
            # Multiple matches
            logger.info(
                "[Multiple-matches]",
                engagement_uuid=engagement_uuid,
                matching_it_users=matching_it_users,
            )
            raise MultipleObjectsReturnedException(
                f"More than one matching 'ObjectGUID' IT user found for "
                f"{employee_uuid=} and {engagement_uuid=}"
            )
        else:
            logger.info(
                "[No-matches]",
                engagement_uuid=engagement_uuid,
                it_users=it_users,
            )
            raise NoObjectsReturnedException("Could not find any matching IT users")

    @staticmethod
    def extract_current_or_latest_object(objects: list[dict]):
        """
        Check the validity in a list of object dictionaries and return the one which
        is either valid today, or has the latest end-date
        """

        if len(objects) == 1:
            return objects[0]
        elif len(objects) == 0:
            raise NoObjectsReturnedException("Objects is empty")
        else:
            # If any of the objects is valid today, return it
            latest_object = None
            for obj in objects:
                valid_to = mo_datestring_to_utc(obj["validity"]["to"])
                valid_from = mo_datestring_to_utc(obj["validity"]["from"])

                if valid_to and valid_from:
                    now_utc = datetime.datetime.utcnow()
                    if now_utc > valid_from and now_utc < valid_to:
                        return obj

                elif not valid_to and valid_from:
                    now_utc = datetime.datetime.utcnow()
                    if now_utc > valid_from:
                        return obj

                elif valid_to and not valid_from:
                    now_utc = datetime.datetime.utcnow()
                    if now_utc < valid_to:
                        return obj

                # Update latest object
                if valid_to:
                    if latest_object:
                        latest_valid_to = mo_datestring_to_utc(
                            latest_object["validity"]["to"]
                        )
                        if latest_valid_to and valid_to > latest_valid_to:
                            latest_object = obj
                    else:
                        latest_object = obj
                else:
                    latest_object = obj

            # Otherwise return the latest
            return latest_object

    async def load_mo_employee(self, uuid: UUID, current_objects_only=True) -> Employee:
        query = gql(
            f"""
            query SingleEmployee {{
              employees(uuids:"{uuid}") {{
                objects {{
                  objects {{
                    uuid
                    cpr_no
                    givenname
                    surname
                    nickname_givenname
                    nickname_surname
                    validity {{
                      to
                      from
                    }}
                  }}
                }}
              }}
            }}
            """
        )

        result = await self.query_past_future_mo(query, current_objects_only)
        entry = self.extract_current_or_latest_object(
            result["employees"]["objects"][0]["objects"]
        )

        entry.pop("validity")

        return Employee(**entry)

    async def load_mo_employees_in_org_unit(
        self, org_unit_uuid: UUID
    ) -> list[Employee]:
        """
        Load all current employees engaged to an org unit
        """
        query = gql(
            f"""
            query EmployeeOrgUnitUUIDs {{
              org_units(uuids: "{org_unit_uuid}") {{
                objects {{
                  objects {{
                    engagements {{
                      employee_uuid
                    }}
                  }}
                }}
              }}
            }}
            """
        )

        result = await self.query_mo(query)
        output = []
        engagement_entries = result["org_units"]["objects"][0]["objects"][0][
            "engagements"
        ]
        for engagement_entry in engagement_entries:
            employee = await self.load_mo_employee(engagement_entry["employee_uuid"])
            output.append(employee)
        return output

    async def load_mo_facet(self, user_key) -> dict:
        query = gql(
            f"""
            query FacetQuery {{
              facets(user_keys: "{user_key}") {{
                objects {{
                  current {{
                    classes {{
                      user_key
                      uuid
                      scope
                      name
                    }}
                  }}
                }}
              }}
            }}
            """
        )
        result = await self.query_mo(query, raise_if_empty=False)

        if len(result["facets"]["objects"]) == 0:
            output = {}
        else:
            output = {
                d["uuid"]: d
                for d in result["facets"]["objects"][0]["current"]["classes"]
            }

        return output

    async def load_mo_facet_uuid(self, user_key: str) -> UUID:
        query = gql(
            f"""
            query FacetUUIDQuery {{
              facets(user_keys: "{user_key}") {{
                objects {{
                  current {{
                    uuid
                  }}
                }}
              }}
            }}
            """
        )
        result = await self.query_mo(query)
        facets = result["facets"]["objects"]
        if len(facets) > 1:
            raise MultipleObjectsReturnedException(
                f"Found multiple facets with user_key = '{user_key}': {result}"
            )
        return UUID(result["facets"]["objects"][0]["current"]["uuid"])

    async def load_mo_employee_address_types(self) -> dict:
        return await self.load_mo_facet("employee_address_type")

    async def load_mo_org_unit_address_types(self) -> dict:
        return await self.load_mo_facet("org_unit_address_type")

    async def load_mo_visibility(self) -> dict:
        return await self.load_mo_facet("visibility")

    async def load_mo_job_functions(self) -> dict:
        return await self.load_mo_facet("engagement_job_function")

    async def load_mo_primary_types(self) -> dict:
        return await self.load_mo_facet("primary_type")

    async def load_mo_engagement_types(self) -> dict:
        return await self.load_mo_facet("engagement_type")

    async def load_mo_org_unit_types(self) -> dict:
        return await self.load_mo_facet("org_unit_type")

    async def load_mo_org_unit_levels(self) -> dict:
        return await self.load_mo_facet("org_unit_level")

    async def load_mo_it_systems(self) -> dict:
        query = gql(
            """
            query ItSystems {
              itsystems {
                objects {
                  current{
                    uuid
                    user_key
                  }
                }
              }
            }
            """
        )
        result = await self.query_mo(query, raise_if_empty=False)

        if len(result["itsystems"]["objects"]) == 0:
            output = {}
        else:
            output = {
                d["current"]["uuid"]: d["current"]
                for d in result["itsystems"]["objects"]
            }

        return output

    async def load_mo_root_org_uuid(self) -> str:
        query = gql(
            """
            query RootOrgUnit {
              org {
                uuid
              }
            }
            """
        )
        uuid: str = (await self.query_mo(query))["org"]["uuid"]
        return uuid

    async def load_mo_org_units(self) -> dict:
        query = gql(
            """
            query OrgUnit {
              org_units(from_date: null, to_date: null) {
                objects {
                  objects {
                    uuid
                    name
                    user_key
                    parent_uuid
                    validity {
                      to
                      from
                    }
                  }
                }
              }
            }
            """
        )
        result = await self.query_mo(query, raise_if_empty=False)

        if len(result["org_units"]["objects"]) == 0:
            output = {}
        else:
            output = {
                d["objects"][0]["uuid"]: self.extract_current_or_latest_object(
                    d["objects"]
                )
                for d in result["org_units"]["objects"]
            }

        return output

    async def load_mo_it_user(self, uuid: UUID, current_objects_only=True) -> ITUser:
        query = gql(
            f"""
            query MyQuery {{
              itusers(uuids: "{uuid}") {{
                objects {{
                  objects {{
                    user_key
                    validity {{
                      from
                      to
                    }}
                    employee_uuid
                    itsystem_uuid
                    engagement_uuid
                  }}
                }}
              }}
            }}
            """
        )

        result = await self.query_past_future_mo(query, current_objects_only)
        entry = self.extract_current_or_latest_object(
            result["itusers"]["objects"][0]["objects"]
        )
        return ITUser.from_simplified_fields(
            user_key=entry["user_key"],
            itsystem_uuid=entry["itsystem_uuid"],
            from_date=entry["validity"]["from"],
            uuid=uuid,
            to_date=entry["validity"]["to"],
            person_uuid=entry["employee_uuid"],
            engagement_uuid=entry["engagement_uuid"],
        )

    async def load_mo_address(
        self, uuid: UUID, current_objects_only: bool = True
    ) -> Address:
        """
        Loads a mo address

        Notes
        ---------
        Only returns addresses which are valid today. Meaning the to/from date is valid.
        """
        query = gql(
            f"""
            query SingleAddress {{
              addresses(uuids: "{uuid}") {{
                objects {{
                  objects {{
                    value: name
                    value2
                    uuid
                    visibility_uuid
                    employee_uuid
                    org_unit_uuid
                    engagement_uuid
                    person: employee {{
                      cpr_no
                    }}
                    validity {{
                      from
                      to
                    }}
                    address_type {{
                      user_key
                      uuid
                    }}
                  }}
                }}
              }}
            }}
            """
        )

        logger.info("[Load-mo-address] Loading address.", uuid=uuid)
        result = await self.query_past_future_mo(query, current_objects_only)

        entry = self.extract_current_or_latest_object(
            result["addresses"]["objects"][0]["objects"]
        )

        address = Address.from_simplified_fields(
            value=entry["value"],
            address_type_uuid=entry["address_type"]["uuid"],
            from_date=entry["validity"]["from"],
            uuid=entry["uuid"],
            to_date=entry["validity"]["to"],
            value2=entry["value2"],
            person_uuid=entry["employee_uuid"],
            visibility_uuid=entry["visibility_uuid"],
            org_unit_uuid=entry["org_unit_uuid"],
            engagement_uuid=entry["engagement_uuid"],
        )

        return address

    async def is_primary(self, engagement_uuid: UUID) -> bool:
        """
        Determine if an engagement is the primary engagement or not.
        """
        query = gql(
            f"""
            query IsPrimary {{
              engagements(uuids: "{engagement_uuid}") {{
                objects {{
                  objects {{
                    is_primary
                  }}
                }}
              }}
            }}
            """
        )

        result = await self.query_mo(query)
        return (
            True
            if result["engagements"]["objects"][0]["objects"][0]["is_primary"]
            else False
        )

    async def load_mo_engagement(
        self,
        uuid: UUID,
        current_objects_only: bool = True,
    ) -> Engagement:
        query = gql(
            f"""
            query SingleEngagement {{
              engagements(uuids: "{uuid}") {{
                objects {{
                  objects {{
                    user_key
                    extension_1
                    extension_2
                    extension_3
                    extension_4
                    extension_5
                    extension_6
                    extension_7
                    extension_8
                    extension_9
                    extension_10
                    leave_uuid
                    primary_uuid
                    job_function_uuid
                    org_unit_uuid
                    engagement_type_uuid
                    employee_uuid
                    validity {{
                      from
                      to
                    }}
                  }}
                }}
              }}
            }}
            """
        )

        logger.info("[Load-mo-engagement] Loading engagement.", uuid=uuid)
        result = await self.query_past_future_mo(query, current_objects_only)

        entry = self.extract_current_or_latest_object(
            result["engagements"]["objects"][0]["objects"]
        )

        engagement = Engagement.from_simplified_fields(
            org_unit_uuid=entry["org_unit_uuid"],
            person_uuid=entry["employee_uuid"],
            job_function_uuid=entry["job_function_uuid"],
            engagement_type_uuid=entry["engagement_type_uuid"],
            user_key=entry["user_key"],
            from_date=entry["validity"]["from"],
            to_date=entry["validity"]["to"],
            uuid=uuid,
            primary_uuid=entry["primary_uuid"],
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
        )
        return engagement

    async def load_mo_employee_addresses(
        self, employee_uuid: UUID, address_type_uuid: UUID
    ) -> list[Address]:
        """
        Loads all current addresses of a specific type for an employee
        """
        query = gql(
            f"""
            query GetEmployeeAddresses {{
              employees(uuids: "{employee_uuid}") {{
                objects {{
                  objects {{
                    addresses(address_types: "{address_type_uuid}") {{
                      uuid
                    }}
                  }}
                }}
              }}
            }}
            """
        )

        result = await self.query_mo(query)

        output = []
        for address_entry in result["employees"]["objects"][0]["objects"][0][
            "addresses"
        ]:
            address = await self.load_mo_address(address_entry["uuid"])
            output.append(address)
        return output

    async def load_mo_org_unit_addresses(
        self, org_unit_uuid, address_type_uuid
    ) -> list[Address]:
        """
        Loads all current addresses of a specific type for an org unit
        """
        query = gql(
            f"""
            query GetOrgUnitAddresses {{
              org_units(uuids: "{org_unit_uuid}") {{
                objects {{
                  objects {{
                    addresses(address_types: "{address_type_uuid}") {{
                      uuid
                    }}
                  }}
                }}
              }}
            }}
            """
        )

        result = await self.query_mo(query)

        output = []
        for address_entry in result["org_units"]["objects"][0]["objects"][0][
            "addresses"
        ]:
            address = await self.load_mo_address(address_entry["uuid"])
            output.append(address)
        return output

    async def load_all_current_it_users(self, it_system_uuid: UUID) -> list[dict]:
        """
        Loads all current it-users
        """
        query = gql(
            """
            query AllEmployees($cursor: Cursor) {
              itusers (limit: 100, cursor: $cursor) {
                objects {
                  current {
                    itsystem_uuid
                    employee_uuid
                    user_key
                  }
                }
                page_info {
                  next_cursor
                }
              }
            }
            """
        )

        result = await self.query_mo_paged(query)

        # Format output
        output = []
        for entry in [r["current"] for r in result["itusers"]["objects"]]:
            if entry["itsystem_uuid"] == str(it_system_uuid):
                output.append(entry)

        return output

    async def load_all_it_users(self, it_system_uuid: UUID) -> list[dict]:
        """
        Loads all it-users in the database. Past, current and future.
        """
        query = gql(
            """
            query AllEmployees($cursor: Cursor) {
              itusers (limit: 100, cursor: $cursor, to_date: null, from_date: null) {
                objects {
                  objects {
                    itsystem_uuid
                    user_key
                  }
                }
                page_info {
                  next_cursor
                }
              }
            }
            """
        )

        result = await self.query_mo_paged(query)

        # Format output
        output = []
        for entries in [r["objects"] for r in result["itusers"]["objects"]]:
            for entry in entries:
                if entry["itsystem_uuid"] == str(it_system_uuid):
                    output.append(entry)

        return output

    async def load_mo_employee_it_users(
        self,
        employee_uuid: UUID,
        it_system_uuid: UUID,
    ) -> list[ITUser]:
        """
        Load all current it users of a specific type linked to an employee
        """
        query = gql(
            f"""
            query ItUserQuery {{
              employees(uuids: "{employee_uuid}") {{
                objects {{
                  objects {{
                    itusers {{
                      uuid
                      itsystem_uuid
                    }}
                  }}
                }}
              }}
            }}
            """
        )

        result = await self.query_mo(query)

        output = []
        for it_user_dict in result["employees"]["objects"][0]["objects"][0]["itusers"]:
            if it_user_dict["itsystem_uuid"] == str(it_system_uuid):
                it_user = await self.load_mo_it_user(it_user_dict["uuid"])
                output.append(it_user)
        return output

    async def load_mo_employee_engagement_dicts(
        self,
        employee_uuid: UUID,
        user_key: str,
    ) -> list[dict]:
        query = gql(
            f"""
            query EngagementQuery {{
              employees(uuids: "{employee_uuid}") {{
                objects {{
                  objects {{
                    engagements(user_keys: "{user_key}") {{
                      uuid
                      user_key
                      org_unit_uuid
                      job_function_uuid
                      engagement_type_uuid
                      primary_uuid
                    }}
                  }}
                }}
              }}
            }}
            """
        )
        try:
            result = await self.query_mo(query)
            output: list[dict] = result["employees"]["objects"][0]["objects"][0][
                "engagements"
            ]
        except NoObjectsReturnedException:
            output = []
        return output

    async def load_mo_employee_engagements(
        self, employee_uuid: UUID
    ) -> list[Engagement]:
        """
        Load all current engagements linked to an employee
        """
        query = gql(
            f"""
            query EngagementQuery {{
              employees(uuids: "{employee_uuid}") {{
                objects {{
                  objects {{
                    engagements {{
                      uuid
                    }}
                  }}
                }}
              }}
            }}
            """
        )

        result = await self.query_mo(query)

        output = []
        for engagement_dict in result["employees"]["objects"][0]["objects"][0][
            "engagements"
        ]:
            engagement = await self.load_mo_engagement(engagement_dict["uuid"])
            output.append(engagement)
        return output

    async def load_all_mo_objects(
        self,
        add_validity: bool = False,
        uuid: str = "",
        object_types_to_try: tuple[str, ...] = (),
        current_objects_only: bool = True,
    ) -> list[dict]:
        """
        Returns a list of dictionaries. One for each object in MO of one of the
        following types:
            - employee
            - org_unit
            - address (either employee or org unit addresses)
            - itusers
            - engagements

        Also adds AMQP object type, service type and payload to the dicts.

        If "uuid" is specified, only returns objects matching this uuid.
        If "object_types_to_try" is also specified, only tries matching the given uuid
        to these object types. "object_types_to_try" needs to be a tuple with strings
        matching self.object_type_dict.keys()
        """

        if add_validity or current_objects_only is False:
            validity_query = """
                             validity {
                                 from
                                 to
                             }
                             """
        else:
            validity_query = ""

        result: dict = {}
        warnings: list[str] = []

        for object_type_to_try in object_types_to_try:
            if object_type_to_try not in self.object_type_dict:
                raise KeyError(
                    f"{object_type_to_try} is not in {self.object_type_dict.keys()}"
                )

        if not object_types_to_try:
            object_types_to_try = tuple(self.object_type_dict.keys())

        if current_objects_only:
            validity_filter = ""
        if not current_objects_only:
            validity_filter = ", to_date: null, from_date: null"

        for object_type in object_types_to_try:
            if object_type in ["employees", "org_units"]:
                additional_uuids = ""
            else:
                additional_uuids = """
                                   org_unit_uuid
                                   employee_uuid
                                   """

            paged_query = gql(
                f"""
                query AllObjects($cursor: Cursor) {{
                    {object_type} (limit: 100, cursor: $cursor {validity_filter}) {{
                        objects {{
                            objects {{
                                uuid
                                {additional_uuids}
                                {validity_query}
                                }}
                            }}
                            page_info {{
                                next_cursor
                            }}
                        }}
                    }}
                """
            )

            query = gql(
                f"""
                query SingleObject {{
                    {object_type} (uuids: "{uuid}" {validity_filter}) {{
                        objects {{
                            objects {{
                                uuid
                                {additional_uuids}
                                {validity_query}
                                }}
                            }}
                        }}
                    }}
                """
            )

            try:
                if uuid:
                    sub_result: dict = await self.query_mo(query, raise_if_empty=False)
                else:
                    sub_result = await self.query_mo_paged(paged_query)

                result = result | sub_result
            except TransportQueryError as e:
                warnings.append(str(e))

        if not result:
            for warning in warnings:
                logger.warning("[Load-all-mo-objects]" + str(warning))

        output = []

        # Determine payload, service type, object type for use in amqp-messages
        for object_type, mo_object_dicts in result.items():
            for mo_object_dict in mo_object_dicts["objects"]:
                mo_object = self.extract_current_or_latest_object(
                    mo_object_dict["objects"]
                )

                # Note that engagements have both employee_uuid and org_unit uuid. But
                # belong to an employee. We handle that by checking for employee_uuid
                # first
                if "employee_uuid" in mo_object and mo_object["employee_uuid"]:
                    parent_uuid = mo_object["employee_uuid"]
                    service_type = "employee"
                elif "org_unit_uuid" in mo_object and mo_object["org_unit_uuid"]:
                    parent_uuid = mo_object["org_unit_uuid"]
                    service_type = "org_unit"
                else:
                    parent_uuid = mo_object["uuid"]
                    if object_type == "employees":
                        service_type = "employee"
                    elif object_type == "org_units":
                        service_type = "org_unit"
                    else:
                        raise InvalidQueryResponse(
                            f"{mo_object} object type '{object_type}' is "
                            "neither 'employees' nor 'org_units'"
                        )

                mo_object["payload"] = UUID(mo_object["uuid"])
                mo_object["parent_uuid"] = UUID(parent_uuid)

                mo_object["object_type"] = self.object_type_dict[object_type]
                mo_object["service_type"] = service_type

                output.append(mo_object)

        if uuid and len(output) > 1:
            raise MultipleObjectsReturnedException(
                f"Found multiple objects with uuid={uuid}"
            )

        return output

    async def load_mo_object(
        self,
        uuid: str,
        object_type: str,
        add_validity: bool = False,
        current_objects_only: bool = True,
    ):
        """
        Returns a mo object as dictionary

        Notes
        -------
        returns None if the object is not a current object or if the object type is not
        defined in self.object_type_dict
        """
        if str(object_type) not in self.supported_object_types:
            return None

        mo_objects = await self.load_all_mo_objects(
            add_validity=add_validity,
            uuid=str(uuid),
            object_types_to_try=(self.object_type_dict_inv[str(object_type)],),
            current_objects_only=current_objects_only,
        )
        if mo_objects:
            # Note: load_all_mo_objects checks if len==1
            return mo_objects[0]
        else:
            raise NoObjectsReturnedException(
                f"{object_type} object with uuid = {uuid} not found"
            )

    async def upload_mo_objects(self, objects: list[Any]):
        """
        Uploads a mo object.
            - If an Employee object is supplied, the employee is updated/created
            - If an Address object is supplied, the address is updated/created
            - And so on...
        """
        model_client = self.user_context["model_client"]
        return cast(list[Any | None], await model_client.upload(objects))

    async def create_or_edit_mo_objects(self, objects: list[tuple[MOBase, Verb]]):
        model_client = self.user_context["model_client"]
        creates, edits = partition(lambda tup: tup[1] == Verb.EDIT, objects)
        create_results = await model_client.upload([obj for obj, verb in creates])
        edit_results = await model_client.edit([obj for obj, verb in edits])
        return cast(list[Any | None], create_results + edit_results)

    async def create_mo_class(
        self,
        name: str,
        user_key: str,
        facet_uuid: UUID,
        scope="",
    ) -> UUID:
        """
        Creates a class in MO

        Returns
        ----------
        uuid: UUID
            The uuid of the created class
        """

        query = gql(
            f"""
            query GetExistingClass{{
              classes(user_keys: "{user_key}") {{
                objects {{
                  objects {{
                    uuid
                  }}
                }}
              }}
            }}
            """
        )
        async with self.create_mo_class_lock:
            existing_classes = await self.query_mo(query, raise_if_empty=False)
            if existing_classes["classes"]["objects"]:
                logger.info("[Create-mo-class] MO class exists.", user_key=user_key)
                return UUID(
                    existing_classes["classes"]["objects"][0]["objects"][0]["uuid"]
                )

            logger.info("[Create-mo-class] Creating MO class.", user_key=user_key)
            query = gql(
                f"""
                mutation CreateClass {{
                  class_create(
                    input: {{name: "{name}",
                            user_key: "{user_key}",
                            facet_uuid: "{facet_uuid}",
                            scope: "{scope}"}}
                  ) {{
                    uuid
                  }}
                }}
                """
            )
            result = await self.query_mo(query)
            return UUID(result["class_create"]["uuid"])

    async def update_mo_class(
        self,
        name: str,
        user_key: str,
        facet_uuid: UUID,
        class_uuid: UUID,
        scope="",
    ) -> UUID:
        """
        Updates a class in MO

        Returns
        ----------
        uuid: UUID
            The uuid of the updated class
        """
        logger.info("[Update-mo-class] Modifying MO class.", user_key=user_key)
        query = gql(
            f"""
            mutation UpdateClass {{
              class_update(
                input: {{name: "{name}",
                        user_key: "{user_key}",
                        facet_uuid: "{facet_uuid}",
                        scope: "{scope}",
                        uuid: "{class_uuid}"}},
                uuid: "{class_uuid}"
              ) {{
                uuid
              }}
            }}
            """
        )
        result = await self.query_mo(query)
        return UUID(result["class_update"]["uuid"])

    async def create_mo_job_function(self, name) -> UUID:
        """
        Creates a job function class in MO

        Returns
        ----------
        uuid: UUID
            The uuid of the created class
        """
        logger.info("[Create-mo-job-function] Creating MO job function.", name=name)
        facet_uuid = await self.load_mo_facet_uuid("engagement_job_function")
        user_key = name
        return await self.create_mo_class(name, user_key, facet_uuid)

    async def create_mo_engagement_type(self, name) -> UUID:
        """
        Creates an engagement type class in MO

        Returns
        ----------
        uuid: UUID
            The uuid of the created class
        """
        logger.info(
            "[Create-mo-engagement-type] Creating MO engagement type", name=name
        )
        facet_uuid = await self.load_mo_facet_uuid("engagement_type")
        user_key = name
        return await self.create_mo_class(name, user_key, facet_uuid)

    async def create_mo_it_system(self, name: str, user_key: str) -> UUID:
        """
        Creates an it-system in MO

        Returns
        ----------
        uuid: UUID
            The uuid of the created it-system
        """
        logger.info("[Create-mo-it-system] Creating MO it-system", user_key=user_key)
        query = gql(
            f"""
            mutation CreateITSystem {{
              itsystem_create(
                input: {{name: "{name}",
                        user_key: "{user_key}"}}
              ) {{
                uuid
              }}
            }}
            """
        )
        result = await self.query_mo(query)
        return UUID(result["itsystem_create"]["uuid"])
