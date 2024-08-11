# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
from uuid import uuid4

from mo_ldap_import_export.autogenerated_graphql_client.client import GraphQLClient
from mo_ldap_import_export.converters import get_org_unit_name_for_parent
from tests.graphql_mocker import GraphQLMocker


async def test_get_org_unit_name_for_parent(graphql_mock: GraphQLMocker) -> None:
    graphql_client = GraphQLClient("http://example.com/graphql")

    ancestors = [
        "Plejecenter Nord",
        "Plejecentre",
        "Sundhed",
        "Kolding Kommune",
    ]

    route = graphql_mock.query("read_org_unit_ancestor_names")
    route.result = {
        "org_units": {
            "objects": [
                {
                    "current": {
                        "name": "Teknik Nord",
                        "ancestors": [{"name": name} for name in ancestors],
                    }
                }
            ]
        }
    }

    # Reversed and None added
    expected_layers = ancestors[::-1] + ["Teknik Nord", None]
    for layer, expected in enumerate(expected_layers):
        name = await get_org_unit_name_for_parent(graphql_client, uuid4(), layer)
        assert name == expected
