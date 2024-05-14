# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
"""Integration tests."""
from uuid import UUID

import pytest
from more_itertools import one

from mo_ldap_import_export.autogenerated_graphql_client import GraphQLClient
from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    ITSystemCreateInput,
)
from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    RAOpenValidityInput,
)


@pytest.mark.integration_test
@pytest.mark.usefixtures("test_client")
async def test_graphql_client(
    graphql_client: GraphQLClient,
) -> None:
    itsystems = await graphql_client.read_itsystems()
    assert itsystems.objects == []

    result = await graphql_client.itsystem_create(
        ITSystemCreateInput(
            user_key="test", name="test", validity=RAOpenValidityInput()
        )
    )
    assert isinstance(result.uuid, UUID)

    itsystems = await graphql_client.read_itsystems()
    itsystem = one(itsystems.objects)
    current = itsystem.current
    assert current is not None
    assert current.uuid == result.uuid
    assert current.user_key == "test"


@pytest.mark.parametrize("counter", list(range(3)))
@pytest.mark.integration_test
async def test_dummy(counter: int) -> None:
    pass
