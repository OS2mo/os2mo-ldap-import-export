# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json
from datetime import datetime
from uuid import UUID
from uuid import uuid4

import pytest
from fastramqpi.context import Context
from pydantic import ValidationError

from mo_ldap_import_export.autogenerated_graphql_client.input_types import (
    ITUserCreateInput,
)
from mo_ldap_import_export.depends import GraphQLClient
from mo_ldap_import_export.environments import generate_username
from mo_ldap_import_export.exceptions import NoObjectsReturnedException
from mo_ldap_import_export.main import create_fastramqpi
from mo_ldap_import_export.moapi import MOAPI
from tests.integration.conftest import AddLdapPerson


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_generate_username_invalid_user(context: Context) -> None:
    dataloader = context["user_context"]["dataloader"]
    with pytest.raises(NoObjectsReturnedException) as exc_info:
        await generate_username(dataloader, uuid4())
    assert "Unable to lookup employee" in str(exc_info.value)


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_generate_username(
    context: Context,
    mo_person: UUID,
) -> None:
    dataloader = context["user_context"]["dataloader"]
    result = await generate_username(dataloader, mo_person)
    assert result == "aag2"


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.parametrize(
    "taken,expected",
    [
        # These all hit the FFFX configuration
        # Picking the 3 first letters of the given name, then adding a number
        # Nothing taken, we generate "aag2"
        (set(), "aag2"),
        # "aag2" taken, we generate "aag3"
        ({"aag2"}, "aag3"),
        # "aag3" taken, but "aag2" free, we generate "aag2"
        ({"aag3"}, "aag2"),
        # Both "aag2" and "aag3" taken, we generate "aag4"
        ({"aag2", "aag3"}, "aag4"),
        # These all hit the LLLX configuration
        # Picking the 3 first letters of the surname, then adding a number
        # aag0 --> aag9 taken, we generate "bac2"
        ({f"aag{i}" for i in range(10)}, "bac2"),
        # aag0 --> aag9 and "bac2" taken, we generate "bac3"
        ({f"aag{i}" for i in range(10)} | {"bac2"}, "bac3"),
    ],
)
@pytest.mark.usefixtures("test_client")
async def test_generate_username_avoids_ldap_taken_names(
    context: Context,
    mo_person: UUID,
    add_ldap_person: AddLdapPerson,
    taken: set[str],
    expected: str,
) -> None:
    # Create an account in LDAP to take away the username
    for userid in taken:
        await add_ldap_person(userid, "0101701234")

    dataloader = context["user_context"]["dataloader"]
    result = await generate_username(dataloader, mo_person)
    assert result == expected


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
@pytest.mark.usefixtures("test_client")
async def test_generate_username_no_available_usernames(
    context: Context,
    mo_person: UUID,
    add_ldap_person: AddLdapPerson,
) -> None:
    for root in ["aag", "bac"]:
        for x in range(10):
            userid = f"{root}{x}"
            await add_ldap_person(userid, "0101701234")

    dataloader = context["user_context"]["dataloader"]
    with pytest.raises(RuntimeError) as exc_info:
        await generate_username(dataloader, mo_person)
    assert "Failed to create user name." in str(exc_info.value)


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": json.dumps(
            {
                "username_generator": {
                    "combinations_to_try": ["FFFX", "LLLX"],
                    # AAG is a slur meaning "awful at gaming", thus it is disallowed in our
                    # pro-noob culture, thus we do not want to generate any usernames using
                    # this base username.
                    "forbidden_usernames": ["aag"],
                }
            }
        ),
    }
)
@pytest.mark.usefixtures("test_client")
async def test_generate_username_forbidden_usernames(
    context: Context,
    mo_person: UUID,
) -> None:
    # Ensure our override was loaded
    settings = context["user_context"]["settings"]
    username_config = settings.conversion_mapping.username_generator
    assert username_config.combinations_to_try == ["FFFX", "LLLX"]
    assert username_config.forbidden_usernames == ["aag"]

    dataloader = context["user_context"]["dataloader"]
    result = await generate_username(dataloader, mo_person)
    # Since we were gonna generate "aag2", via the FFFX policy, but "aag" is disallowed,
    # we must generate a username using the LLLX policy, giving us "bac2"
    assert result == "bac2"


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": json.dumps(
            {
                "username_generator": {
                    "combinations_to_try": ["FLXX"],
                }
            }
        ),
    }
)
@pytest.mark.usefixtures("test_client")
async def test_generate_username_use_fxxx_combination(
    context: Context,
    mo_person: UUID,
    add_ldap_person: AddLdapPerson,
) -> None:
    # Ensure our override was loaded
    settings = context["user_context"]["settings"]
    assert settings.conversion_mapping.username_generator.combinations_to_try == [
        "FLXX"
    ]

    dataloader = context["user_context"]["dataloader"]
    result = await generate_username(dataloader, mo_person)
    assert result == "ab22"

    await add_ldap_person("ab22", "0101701234")

    dataloader = context["user_context"]["dataloader"]
    result = await generate_username(dataloader, mo_person)
    assert result == "ab33"


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
        "CONVERSION_MAPPING": json.dumps(
            {
                "username_generator": {
                    # Setting both "disallow_mo_usernames" and "remove_vowels" is
                    # equivalent to the old "AlleroedUserNameGenerator"
                    # disallow_mo_usernames must be configured using the
                    # `existing_username_itsystem` key which points to the user-key of
                    # the IT-system containing the reserved LDAP usernames.
                    # For testing we use the existing ADtitle system.
                    "disallow_mo_usernames": "True",
                    "existing_usernames_itsystem": "ADtitle",
                    "remove_vowels": "True",
                    "combinations_to_try": ["FFFX", "LLLX"],
                }
            }
        ),
    }
)
@pytest.mark.parametrize(
    "start,end",
    [
        # Past
        (datetime(1970, 1, 1), datetime(1980, 1, 1)),
        # Current
        (datetime(1990, 1, 1), None),
        # Future
        (datetime(3000, 1, 1), None),
    ],
)
@pytest.mark.parametrize(
    "taken,expected",
    # All of these tests-cases are almost identical to the avoid_ldap_taken_names ones
    # This is purposeful as we wish to check that the two functionalities work the
    # same and the only difference is the source for the illegal names
    # There are unfortunately differences due to vowel removal though, but only in
    # the last two tests. In the future this should be changed.
    # See the TODO about this on the UsernameGeneratorConfig in config.py
    [
        # These all hit the FFFX configuration
        # Picking the 3 first letters of the given name, then adding a number
        # Nothing taken, we generate "aag2"
        (set(), "aag2"),
        # "aag2" taken, we generate "aag3"
        ({"aag2"}, "aag3"),
        # "aag3" taken, but "aag2" free, we generate "aag2"
        ({"aag3"}, "aag2"),
        # Both "aag2" and "aag3" taken, we generate "aag4"
        ({"aag2", "aag3"}, "aag4"),
        # These all hit the LLLX configuration
        # Picking the 3 first letters of the surname, then adding a number
        # aag0 --> aag9 taken, we generate "bch2"
        ({f"aag{i}" for i in range(10)}, "bch2"),
        # aag0 --> aag9 and "bch2" taken, we generate "bch3"
        ({f"aag{i}" for i in range(10)} | {"bch2"}, "bch3"),
    ],
)
@pytest.mark.usefixtures("test_client")
async def test_generate_username_avoids_mo_taken_names(
    graphql_client: GraphQLClient,
    mo_api: MOAPI,
    context: Context,
    start: datetime,
    end: datetime | None,
    mo_person: UUID,
    taken: set[str],
    expected: str,
) -> None:
    it_system_uuid = UUID(await mo_api.get_it_system_uuid("ADtitle"))
    # Create an IT-user MO to take away the username
    for userid in taken:
        await graphql_client.ituser_create(
            ITUserCreateInput(
                person=mo_person,
                user_key=userid,
                itsystem=it_system_uuid,
                validity={"from": start, "to": end},
            )
        )

    dataloader = context["user_context"]["dataloader"]
    result = await generate_username(dataloader, mo_person)
    assert result == expected


@pytest.mark.integration_test
@pytest.mark.envvar(
    {
        "LISTEN_TO_CHANGES_IN_MO": "False",
        "LISTEN_TO_CHANGES_IN_LDAP": "False",
    }
)
async def test_generate_username_use_invalid_combinations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with monkeypatch.context() as m:
        m.setenv(
            "CONVERSION_MAPPING",
            json.dumps(
                {
                    "username_generator": {
                        "combinations_to_try": ["INVALID", "CHARACTERS"],
                    },
                }
            ),
        )
        # Construct an app and see it explode during construction
        with pytest.raises(ValidationError) as exc_info:
            create_fastramqpi()

    error_strings = [
        "1 validation error for Settings",
        "conversion_mapping -> username_generator -> combinations_to_try",
        "Incorrect combination found: 'INVALID'",
        "combinations can only contain ['F', 'L', '1', '2', '3', 'X']",
    ]
    for error in error_strings:
        assert error in str(exc_info.value)
