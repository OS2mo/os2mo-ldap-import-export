# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0

import csv
from io import StringIO
from uuid import UUID

import pytest
from httpx import AsyncClient


@pytest.mark.integration_test
async def test_ldap_template_all(test_client: AsyncClient, mo_person: UUID) -> None:
    response = await test_client.get("/Inspect/mo2ldap/all")
    assert response.status_code == 200
    stream = StringIO(response.text)
    reader = csv.DictReader(stream, dialect=csv.unix_dialect)
    assert list(reader) == [
        {
            "givenName": "Aage",
            "sn": "Bach Klarskov",
            "employeeNumber": "2108613133",
            "__mo_uuid": str(mo_person),
            "title": str(mo_person),
        }
    ]


@pytest.mark.integration_test
@pytest.mark.usefixtures("ldap_person")
async def test_ldap_template_existent(
    test_client: AsyncClient, mo_person: UUID
) -> None:
    response = await test_client.get("/Inspect/mo2ldap/all")
    assert response.status_code == 200
    stream = StringIO(response.text)
    reader = csv.DictReader(stream, dialect=csv.unix_dialect)
    assert list(reader) == [
        {
            "__mo_uuid": str(mo_person),
            "title": str(mo_person),
        }
    ]
