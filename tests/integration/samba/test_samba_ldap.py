# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json

import pytest
from fastramqpi.context import Context

from mo_ldap_import_export.config import Settings
from mo_ldap_import_export.ldap import configure_ldap_connection
from mo_ldap_import_export.ldap_event_generator import MICROSOFT_EPOCH
from mo_ldap_import_export.ldap_event_generator import LDAPEventGenerator
from mo_ldap_import_export.ldapapi import LDAPAPI
from mo_ldap_import_export.utils import combine_dn_strings

SAMBA_HOST = "samba"
SAMBA_PORT = 389

SAMBA_ENVVARS = {
    "LDAP_CONTROLLERS": json.dumps([{"host": SAMBA_HOST, "port": SAMBA_PORT}]),
    "LDAP_DOMAIN": "magenta.dk",
    "LDAP_USER": "cn=admin,dc=magenta,dc=dk",
    "LDAP_PASSWORD": "AdminPassword123",
    "LDAP_SEARCH_BASE": "dc=magenta,dc=dk",
    "LDAP_OUS_TO_SEARCH_IN": json.dumps(["ou=os2mo,o=magenta"]),
    "LDAP_OU_FOR_NEW_USERS": "ou=os2mo,o=magenta",
    "LDAP_DIALECT": "Standard",
    "LDAP_AUTH_METHOD": "simple",
}


@pytest.fixture
async def write_ldap_api(load_marked_envvars: None) -> LDAPAPI:
    """Override to make ldap_org_unit use the test's envvars (pointing to samba)."""
    settings = Settings()
    return LDAPAPI(settings, configure_ldap_connection(settings))


@pytest.mark.integration_test
@pytest.mark.envvar(SAMBA_ENVVARS)
@pytest.mark.usefixtures("test_client")
async def test_samba_ldap_create_and_read_persons(
    context: Context,
    ldap_org_unit: list[str],
) -> None:
    """Test creating persons in Samba LDAP and reading them back."""
    ldap_connection = context["user_context"]["dataloader"].ldapapi.ldap_connection

    persons = [
        {
            "uid": "jdoe",
            "cn": "John Doe",
            "sn": "Doe",
            "givenName": "John",
            "mail": "john.doe@example.com",
        },
        {
            "uid": "asmith",
            "cn": "Alice Smith",
            "sn": "Smith",
            "givenName": "Alice",
            "mail": "alice.smith@example.com",
        },
    ]

    created_dns: list[list[str]] = []
    try:
        for person in persons:
            person_dn = [f"uid={person['uid']}"] + ldap_org_unit
            await ldap_connection.ldap_add(
                combine_dn_strings(person_dn),
                object_class=["top", "person", "organizationalPerson", "inetOrgPerson"],
                attributes={
                    "objectClass": [
                        "top",
                        "person",
                        "organizationalPerson",
                        "inetOrgPerson",
                    ],
                    **person,
                },
            )
            created_dns.append(person_dn)

        # Read back and verify
        for person in persons:
            search_base = combine_dn_strings(ldap_org_unit)
            response, result = await ldap_connection.ldap_search(
                search_base=search_base,
                search_filter=f"(uid={person['uid']})",
                attributes=["uid", "cn", "sn", "givenName", "mail"],
            )
            assert len(response) == 1, (
                f"Expected 1 entry for {person['uid']}, got {len(response)}"
            )

            entry = response[0]["attributes"]
            # Attributes may be returned as lists
            assert entry["uid"] == [person["uid"]] or entry["uid"] == person["uid"]
            assert entry["cn"] == [person["cn"]] or entry["cn"] == person["cn"]
            assert entry["sn"] == [person["sn"]] or entry["sn"] == person["sn"]
            assert (
                entry["givenName"] == [person["givenName"]]
                or entry["givenName"] == person["givenName"]
            )
            assert entry["mail"] == [person["mail"]] or entry["mail"] == person["mail"]

    finally:
        for dn in created_dns:
            await ldap_connection.ldap_delete(combine_dn_strings(dn))


@pytest.mark.integration_test
@pytest.mark.envvar(SAMBA_ENVVARS)
@pytest.mark.usefixtures("test_client")
async def test_ldap_event_generator_detects_samba_changes(
    context: Context,
    ldap_org_unit: list[str],
) -> None:
    """Test that LDAPEventGenerator can detect changes made in Samba."""
    settings = context["user_context"]["settings"]
    search_base = combine_dn_strings(ldap_org_unit)

    sessionmaker = context["sessionmaker"]
    graphql_client = context["graphql_client"]
    ldap_connection = context["user_context"]["dataloader"].ldapapi.ldap_connection

    event_generator = LDAPEventGenerator(
        sessionmaker=sessionmaker,
        settings=settings,
        graphql_client=graphql_client,
        ldap_connection=ldap_connection.connection,
    )

    uuids_before, _ = await event_generator.poll(search_base, MICROSOFT_EPOCH)
    initial_count = len(uuids_before)

    # Create a new person
    person_dn = ["uid=testuser"] + ldap_org_unit
    await ldap_connection.ldap_add(
        combine_dn_strings(person_dn),
        object_class=["top", "person", "organizationalPerson", "inetOrgPerson"],
        attributes={
            "objectClass": ["top", "person", "organizationalPerson", "inetOrgPerson"],
            "uid": "testuser",
            "cn": "Test User",
            "sn": "User",
            "givenName": "Test",
            "mail": "test.user@example.com",
        },
    )

    try:
        uuids_after, timestamp = await event_generator.poll(search_base, MICROSOFT_EPOCH)
        assert len(uuids_after) == initial_count + 1
        assert timestamp is not None

        # Verify we can find the person's UUID in detected changes
        response, _ = await ldap_connection.ldap_search(
            search_base=combine_dn_strings(ldap_org_unit),
            search_filter="(uid=testuser)",
            attributes=[settings.ldap_unique_id_field],
        )
        expected_uuid = str(response[0]["attributes"][settings.ldap_unique_id_field])

        detected_uuids = {str(uuid) for uuid in uuids_after}
        assert expected_uuid in detected_uuids
    finally:
        await ldap_connection.ldap_delete(combine_dn_strings(person_dn))
