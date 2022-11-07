# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Dataloaders to bulk requests."""
from functools import partial
from typing import Any
from typing import Callable
from typing import cast
from typing import Union

import structlog
from fastramqpi.context import Context
from gql import gql
from gql.client import AsyncClientSession
from ldap3 import Connection
from more_itertools import flatten
from pydantic import BaseModel
from raclients.modelclient.mo import ModelClient
from ramodels.mo.employee import Employee
from strawberry.dataloader import DataLoader


class Dataloaders(BaseModel):
    """Collection of program dataloaders."""

    class Config:
        """Arbitrary types need to be allowed to have DataLoader members."""

        arbitrary_types_allowed = True

    ad_employees_loader: DataLoader
    ad_employee_loader: DataLoader
    ad_employees_uploader: DataLoader
    mo_employees_loader: DataLoader
    mo_employee_uploader: DataLoader
    mo_employee_loader: DataLoader


class AdEmployee(BaseModel):
    """Model for an AD employee"""

    dn: str
    Name: str  # TODO: This field cannot be modified in AD. Add a 'protected' flag?
    Department: Union[str, None]


def get_ad_attributes() -> list[str]:
    return [a for a in AdEmployee.schema()["properties"].keys() if a != "dn"]


async def load_ad_employee(
    keys: list[str], ad_connection: Connection
) -> list[AdEmployee]:

    logger = structlog.get_logger()
    output = []

    for dn in keys:
        searchParameters = {
            "search_base": dn,
            "search_filter": "(objectclass=organizationalPerson)",
            "attributes": get_ad_attributes(),
        }

        ad_connection.search(**searchParameters)
        response = ad_connection.response

        if len(response) > 1:
            raise Exception("Found multiple entries for dn=%s" % dn)
        elif len(response) == 0:
            raise Exception("Found no entries for dn=%s" % dn)

        for attribute, value in response[0]["attributes"].items():
            if value == []:
                response[0]["attributes"][attribute] = None

        employee = AdEmployee(
            dn=response[0]["dn"],
            Name=response[0]["attributes"]["name"],
            Department=response[0]["attributes"]["department"],
        )

        logger.info("Found %s" % employee)
        output.append(employee)

    return output


async def load_ad_employees(
    key: int,
    ad_connection: Connection,
    search_base: str,
) -> list[list[AdEmployee]]:
    """
    Returns list with all organizationalPersons
    """
    logger = structlog.get_logger()
    output = []

    searchParameters = {
        "search_base": search_base,
        "search_filter": "(objectclass=organizationalPerson)",
        "attributes": get_ad_attributes(),
        "paged_size": 500,  # TODO: Find this number from AD rather than hard-code it?
    }

    page = 0
    while True:
        logger.info("searching page %d" % page)
        page += 1
        ad_connection.search(**searchParameters)
        output.extend(ad_connection.entries)

        # TODO: Skal "1.2.840.113556.1.4.319" være Configurerbar?
        cookie = ad_connection.result["controls"]["1.2.840.113556.1.4.319"]["value"][
            "cookie"
        ]

        if cookie and type(cookie) is bytes:
            searchParameters["paged_cookie"] = cookie
        else:
            break

    output_list = [
        AdEmployee(
            dn=o.entry_dn,
            Name=o.Name.value,
            Department=o.Department.value,
        )
        for o in output
    ]

    return [output_list]


async def upload_ad_employee(keys: list[AdEmployee], ad_connection: Connection):
    logger = structlog.get_logger()
    output = []
    success = 0
    failed = 0
    for key in keys:
        dn = key.dn
        parameters_to_upload = [k for k in key.dict().keys() if k != "dn"]
        results = []
        for parameter_to_upload in parameters_to_upload:
            value = key.dict()[parameter_to_upload]
            value_to_upload = [] if value is None else [value]
            changes = {parameter_to_upload: [("MODIFY_REPLACE", value_to_upload)]}

            logger.info("Uploading the following changes: %s" % changes)
            ad_connection.modify(dn, changes)
            response = ad_connection.result

            # If the user does not exist, create him/her/hir
            if response["description"] == "noSuchObject":
                logger.info("Creating %s" % dn)
                ad_connection.add(dn, "organizationalPerson")
                ad_connection.modify(dn, changes)
                response = ad_connection.result

            if response["description"] == "success":
                success += 1
            else:
                failed += 1
            logger.info("Response: %s" % response)

            results.append(response)

        output.append(results)

    logger.info("Succeeded MODIFY_REPLACE operations: %d" % success)
    logger.info("Failed MODIFY_REPLACE operations: %d" % failed)
    return output


def get_mo_employee_objects_str():
    """
    Returns object-names-of-interest for the MO employee object
    """
    objects = [
        "uuid",
        "cpr_no",
        "givenname",
        "surname",
        "nickname_givenname",
        "nickname_surname",
    ]
    objects_str = ", ".join(objects)
    return objects_str


def format_employee_output(result):
    output = []
    for entry in list(flatten([r["objects"] for r in result["employees"]])):
        output.append(Employee(**entry))
    return output


async def load_mo_employees(
    key: int, graphql_session: AsyncClientSession
) -> list[list[Employee]]:

    query = gql(
        """
        query AllEmployees {
          employees {
            objects {
              %s
            }
          }
        }
        """
        % get_mo_employee_objects_str()
    )

    result = await graphql_session.execute(query)
    return [format_employee_output(result)]


async def load_mo_employee(
    keys: list[str], graphql_session: AsyncClientSession
) -> list[Employee]:
    output = []
    for uuid in keys:
        query = gql(
            """
            query SinlgeEmployee {
              employees(uuids:"{%s}") {
                objects {
                  %s
                }
              }
            }
            """
            % (uuid, get_mo_employee_objects_str())
        )

        result = await graphql_session.execute(query)
        output.extend(format_employee_output(result))

    return output


async def upload_mo_employee(
    keys: list[Employee],
    model_client: ModelClient,
):
    # return await model_client.upload(keys)
    return cast(list[Any | None], await model_client.upload(keys))


def configure_dataloaders(context: Context) -> Dataloaders:
    """Construct our dataloaders from the FastRAMQPI context.

    Args:
        context: The FastRAMQPI context to configure our dataloaders with.

    Returns:
        Dataloaders required for ensure_adguid_itsystem.
    """

    graphql_loader_functions: dict[str, Callable] = {
        "mo_employees_loader": load_mo_employees,
        "mo_employee_loader": load_mo_employee,
    }

    graphql_session = context["user_context"]["gql_client"]
    graphql_dataloaders: dict[str, DataLoader] = {
        key: DataLoader(
            load_fn=partial(value, graphql_session=graphql_session), cache=False
        )
        for key, value in graphql_loader_functions.items()
    }

    model_client = context["user_context"]["model_client"]
    mo_employee_uploader = DataLoader(
        load_fn=partial(upload_mo_employee, model_client=model_client),
        cache=False,
    )

    settings = context["user_context"]["settings"]
    ad_connection = context["user_context"]["ad_connection"]
    ad_employees_loader = DataLoader(
        load_fn=partial(
            load_ad_employees,
            ad_connection=ad_connection,
            search_base=settings.ad_search_base,
        ),
        cache=False,
    )

    ad_employee_loader = DataLoader(
        load_fn=partial(load_ad_employee, ad_connection=ad_connection),
        cache=False,
    )

    ad_employees_uploader = DataLoader(
        load_fn=partial(upload_ad_employee, ad_connection=ad_connection),
        cache=False,
    )

    return Dataloaders(
        **graphql_dataloaders,
        ad_employees_loader=ad_employees_loader,
        ad_employee_loader=ad_employee_loader,
        ad_employees_uploader=ad_employees_uploader,
        mo_employee_uploader=mo_employee_uploader,
    )
