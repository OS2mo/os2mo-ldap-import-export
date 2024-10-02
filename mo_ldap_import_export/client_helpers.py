# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
"""Collection of helper functions for the GraphQLClient."""

from more_itertools import one

from .autogenerated_graphql_client.client import GraphQLClient
from .exceptions import UUIDNotFoundException


async def get_it_system_uuid(
    graphql_client: GraphQLClient, itsystem_user_key: str
) -> str:
    result = await graphql_client.read_itsystem_uuid(itsystem_user_key)
    exception = UUIDNotFoundException(
        f"itsystem not found, user_key: {itsystem_user_key}"
    )
    return str(one(result.objects, too_short=exception).uuid)
