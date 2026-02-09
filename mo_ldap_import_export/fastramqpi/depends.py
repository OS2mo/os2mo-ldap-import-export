# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from functools import cache
from typing import Annotated
from typing import Any
from typing import AsyncIterator
from typing import Callable

from authlib.integrations.httpx_client import AsyncOAuth2Client
from fastapi import Depends
from gql.client import AsyncClientSession
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker

from .raclients.graph.client import GraphQLClient as _GraphQLClient
from .raclients.modelclient.mo import ModelClient as _ModelClient
from .ramqp.depends import from_context
from .ramqp.mo import MOAMQPSystem as _MOAMQPSystem


@cache
def from_user_context(field: str) -> Callable[..., Any]:
    """Construct a Callable which extracts 'field' from the FastRAMQPI user context.

    Args:
        field: The field to extract.

    Returns:
        A callable which extracts 'field' from the FastRAMQPI user context.
    """

    def inner(user_context: UserContext) -> Any:
        return user_context[field]

    return inner


MOAMQPSystem = Annotated[_MOAMQPSystem, Depends(from_context("amqpsystem"))]


async def get_session(sessionmaker: "Sessionmaker") -> AsyncIterator[AsyncSession]:
    async with sessionmaker() as session, session.begin():
        yield session


Engine = Annotated[AsyncEngine, Depends(from_context("engine"))]
Sessionmaker = Annotated[async_sessionmaker, Depends(from_context("sessionmaker"))]
Session = Annotated[AsyncSession, Depends(get_session)]

LegacyGraphQLClient = Annotated[
    _GraphQLClient, Depends(from_context("legacy_graphql_client"))
]
LegacyGraphQLSession = Annotated[
    AsyncClientSession, Depends(from_context("legacy_graphql_session"))
]
LegacyModelClient = Annotated[
    _ModelClient, Depends(from_context("legacy_model_client"))
]

MOClient = Annotated[AsyncOAuth2Client, Depends(from_context("mo_client"))]

UserContext = Annotated[dict[str, Any], Depends(from_context("user_context"))]
