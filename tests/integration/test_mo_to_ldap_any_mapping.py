# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json
from uuid import uuid4

import pytest
from fastapi.encoders import jsonable_encoder
from fastramqpi.events import Event
from httpx import AsyncClient

from mo_ldap_import_export.ldapapi import LDAPAPI


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": json.dumps(
            {
                "mo_to_ldap": [
                    {
                        "identifier": "known",
                        "routing_key": "person",
                        "object_class": "inetOrgPerson",
                        "template": "empty",
                    }
                ]
            }
        ),
    }
)
@pytest.mark.parametrize(
    "url,status_code",
    [
        ("/mo_to_ldap", 404),
        ("/mo_to_ldap/", 404),
        ("/mo_to_ldap/unknown", 404),
        ("/mo_to_ldap/known", 422),
    ],
)
async def test_endpoint_setup(
    test_client: AsyncClient, url: str, status_code: int
) -> None:
    result = await test_client.post(url)
    assert result.status_code == status_code


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.parametrize(
    "expected",
    [
        pytest.param(
            "Unable to parse Jinja template output as JSON",
            marks=pytest.mark.envvar(
                {
                    "CONVERSION_MAPPING": json.dumps(
                        {
                            "mo_to_ldap": [
                                {
                                    "identifier": "known",
                                    "routing_key": "person",
                                    "object_class": "inetOrgPerson",
                                    # This is not JSON
                                    "template": "empty",
                                }
                            ]
                        }
                    )
                }
            ),
        ),
        pytest.param(
            "Unable to parse Jinja template output as model",
            marks=pytest.mark.envvar(
                {
                    "CONVERSION_MAPPING": json.dumps(
                        {
                            "mo_to_ldap": [
                                {
                                    "identifier": "known",
                                    "routing_key": "person",
                                    "object_class": "inetOrgPerson",
                                    # This does not fulfill the JinjaOutput model
                                    # Missing 'dn', 'create' and 'attributes'
                                    "template": "{{ {}|tojson }}",
                                }
                            ]
                        }
                    )
                }
            ),
        ),
        pytest.param(
            "Unable to parse Jinja template output as model",
            marks=pytest.mark.envvar(
                {
                    "CONVERSION_MAPPING": json.dumps(
                        {
                            "mo_to_ldap": [
                                {
                                    "identifier": "known",
                                    "routing_key": "person",
                                    "object_class": "inetOrgPerson",
                                    # This does not fulfill the JinjaOutput model
                                    # Missing 'attributes'
                                    "template": """
                                {{
                                    {
                                        "dn": "CN=foo",
                                        "create": false,
                                    }|tojson
                                }}
                                """,
                                }
                            ]
                        }
                    )
                }
            ),
        ),
        pytest.param(
            "Unable to parse Jinja template output as model",
            marks=pytest.mark.envvar(
                {
                    "CONVERSION_MAPPING": json.dumps(
                        {
                            "mo_to_ldap": [
                                {
                                    "identifier": "known",
                                    "routing_key": "person",
                                    "object_class": "inetOrgPerson",
                                    # This does not fulfill the JinjaOutput model
                                    # 'attributes' has the wrong type
                                    "template": """
                                {{
                                    {
                                        "dn": "CN=foo",
                                        "create": false,
                                        "attributes": "hello"
                                    }|tojson
                                }}
                                """,
                                }
                            ]
                        }
                    )
                }
            ),
        ),
        pytest.param(
            "Unable to parse Jinja template output as model",
            marks=pytest.mark.envvar(
                {
                    "CONVERSION_MAPPING": json.dumps(
                        {
                            "mo_to_ldap": [
                                {
                                    "identifier": "known",
                                    "routing_key": "person",
                                    "object_class": "inetOrgPerson",
                                    # This does not fulfills the JinjaOutput model
                                    # 'one_field_too_many' is an unexpected field
                                    "template": """
                                    {{
                                        {
                                            "dn": "CN=foo,o=magenta,dc=magenta,dc=dk",
                                            "create": true,
                                            "attributes": {"sn": "Lathe"},
                                            "one_field_too_many": "true"
                                        }|tojson
                                    }}
                                    """,
                                }
                            ]
                        }
                    )
                }
            ),
        ),
        pytest.param(
            "Unable to find Jinja referenced dn",
            marks=pytest.mark.envvar(
                {
                    "CONVERSION_MAPPING": json.dumps(
                        {
                            "mo_to_ldap": [
                                {
                                    "identifier": "known",
                                    "routing_key": "person",
                                    "object_class": "inetOrgPerson",
                                    # This fulfills the JinjaOutput model
                                    # However the referenced DN does not exist
                                    "template": """
                                {{
                                    {
                                        "dn": "CN=foo",
                                        "create": false,
                                        "attributes": {}
                                    }|tojson
                                }}
                                """,
                                }
                            ]
                        }
                    )
                }
            ),
        ),
        pytest.param(
            "The LDAP server was unwilling to perform the change",
            marks=pytest.mark.envvar(
                {
                    "CONVERSION_MAPPING": json.dumps(
                        {
                            "mo_to_ldap": [
                                {
                                    "identifier": "known",
                                    "routing_key": "person",
                                    "object_class": "inetOrgPerson",
                                    # This fulfills the JinjaOutput model and is create
                                    # However the requested DN is incomplete
                                    "template": """
                                {{
                                    {
                                        "dn": "CN=foo",
                                        "create": true,
                                        "attributes": {}
                                    }|tojson
                                }}
                                """,
                                }
                            ]
                        }
                    )
                }
            ),
        ),
        pytest.param(
            "The LDAP server states that required attributes are missing",
            marks=pytest.mark.envvar(
                {
                    "CONVERSION_MAPPING": json.dumps(
                        {
                            "mo_to_ldap": [
                                {
                                    "identifier": "known",
                                    "routing_key": "person",
                                    "object_class": "inetOrgPerson",
                                    # This fulfills the JinjaOutput model and is create
                                    # The DN format is also complete
                                    # However the required attributes are missing
                                    "template": """
                                {{
                                    {
                                        "dn": "CN=foo,o=magenta,dc=magenta,dc=dk",
                                        "create": true,
                                        "attributes": {}
                                    }|tojson
                                }}
                                """,
                                }
                            ]
                        }
                    )
                }
            ),
        ),
        pytest.param(
            "The LDAP server could not find the superior",
            marks=pytest.mark.envvar(
                {
                    "CONVERSION_MAPPING": json.dumps(
                        {
                            "mo_to_ldap": [
                                {
                                    "identifier": "known",
                                    "routing_key": "person",
                                    "object_class": "inetOrgPerson",
                                    # This fulfills the JinjaOutput model and is create
                                    # The DN format is also complete, also attributes are OK
                                    # However the referenced DC and O are missing
                                    "template": """
                                {{
                                    {
                                        "dn": "CN=foo,o=magenta,dc=magenta,dc=dk",
                                        "create": true,
                                        "attributes": {
                                            "sn": "Lathe"
                                        }
                                    }|tojson
                                }}
                                """,
                                }
                            ]
                        }
                    )
                }
            ),
        ),
    ],
)
async def test_endpoint_handler_failures(
    test_client: AsyncClient, expected: str
) -> None:
    uuid = uuid4()
    payload = jsonable_encoder(Event(subject=uuid, priority=10))
    result = await test_client.post("/mo_to_ldap/known", json=payload)
    assert result.status_code == 500
    assert result.json() == {"detail": {"message": expected}}


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "LDAP_READ_ONLY": "True",
        "CONVERSION_MAPPING": json.dumps(
            {
                "mo_to_ldap": [
                    {
                        "identifier": "known",
                        "routing_key": "person",
                        "object_class": "inetOrgPerson",
                        # This fulfills the JinjaOutput model and is create
                        # The DN format is also complete, also attributes are OK
                        # However the referenced DC and O are missing
                        "template": """
                    {{
                        {
                            "dn": "CN=foo,o=magenta,dc=magenta,dc=dk",
                            "create": true,
                            "attributes": {
                                "sn": "Lathe"
                            }
                        }|tojson
                    }}
                    """,
                    }
                ]
            }
        ),
    }
)
async def test_endpoint_handler_read_only(test_client: AsyncClient) -> None:
    uuid = uuid4()
    payload = jsonable_encoder(Event(subject=uuid, priority=10))
    result = await test_client.post("/mo_to_ldap/known", json=payload)
    assert result.status_code == 200
    assert result.json() == {
        "detail": {
            "message": "LDAP connection is read-only",
            "dn": "CN=foo,o=magenta,dc=magenta,dc=dk",
            "old_state": None,
            "requested_state": {"sn": ["Lathe"]},
        }
    }


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": json.dumps(
            {
                "mo_to_ldap": [
                    {
                        "identifier": "known",
                        "routing_key": "person",
                        "object_class": "inetOrgPerson",
                        "template": """
                        {{
                            {
                                "dn": "CN=foo,o=magenta,dc=magenta,dc=dk",
                                "create": true,
                                "attributes": {
                                    "cn": "bar"
                                }
                            }|tojson
                        }}
                        """,
                    }
                ]
            }
        ),
    }
)
async def test_mismatched_attributes(test_client: AsyncClient) -> None:
    uuid = uuid4()
    payload = jsonable_encoder(Event(subject=uuid, priority=10))
    result = await test_client.post("/mo_to_ldap/known", json=payload)
    assert result.status_code, result.json() == (
        500,
        {"detail": "Mismatched attributes {'cn'}"},
    )


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": json.dumps(
            {
                "mo_to_ldap": [
                    {
                        "identifier": "known",
                        "routing_key": "person",
                        "object_class": "inetOrgPerson",
                        # This fulfills the JinjaOutput model and is create
                        # The DN format is also complete, also attributes are OK
                        # However the referenced DC and O are missing
                        "template": """
                        {{
                            {
                                "dn": "CN=foo,o=magenta,dc=magenta,dc=dk",
                                "create": true,
                                "attributes": {
                                    "sn": "Lathe"
                                }
                            }|tojson
                        }}
                        """,
                    }
                ]
            }
        ),
    }
)
@pytest.mark.usefixtures("ldap_org")
async def test_endpoint_handler(test_client: AsyncClient, ldap_api: LDAPAPI) -> None:
    uuid = uuid4()
    payload = jsonable_encoder(Event(subject=uuid, priority=10))
    result = await test_client.post("/mo_to_ldap/known", json=payload)
    assert result.status_code == 200
    assert result.json() is None

    dn = "cn=foo,o=magenta,dc=magenta,dc=dk"
    obj = await ldap_api.get_object_by_dn(dn)
    assert obj.dn == dn
    assert hasattr(obj, "cn")
    assert obj.cn == ["foo"]
    assert hasattr(obj, "sn")
    assert obj.sn == ["Lathe"]
    assert hasattr(obj, "objectClass")
    assert obj.objectClass == ["inetOrgPerson"]
