# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json

import pytest
from httpx import AsyncClient


@pytest.mark.integration_test
@pytest.mark.envvar({"LISTEN_TO_CHANGES_IN_MO": "False"})
async def test_inspect_mo_to_ldap_unknown_identifier(test_client: AsyncClient) -> None:
    response = await test_client.get("/Inspect/mo_to_ldap/unknown/all")
    assert response.status_code == 404
    assert response.json()["detail"] == "Unknown identifier: unknown"


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "CONVERSION_MAPPING": json.dumps(
            {
                "mo_to_ldap": {
                    "address_handler": {
                        "routing_key": "address",
                        "object_class": "inetOrgPerson",
                        "template": "{{ skip_if_none(None) }}",
                    }
                }
            }
        ),
    }
)
async def test_inspect_mo_to_ldap_unsupported_routing_key(
    test_client: AsyncClient,
) -> None:
    response = await test_client.get("/Inspect/mo_to_ldap/address_handler/all")
    assert response.status_code == 404
    assert response.json()["detail"] == "Unsupported routing key: address"
