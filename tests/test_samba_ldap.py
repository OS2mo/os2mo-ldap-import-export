# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import pytest
from ldap3 import ALL
from ldap3 import Connection
from ldap3 import Server

SAMBA_HOST = "localhost"
SAMBA_PORT = 3890
SAMBA_ADMIN_DN = "cn=admin,dc=samba,dc=local"
SAMBA_ADMIN_PASSWORD = "adminpassword"
SAMBA_BASE_DN = "dc=samba,dc=local"


@pytest.fixture
def ldap_connection():
    """Create a connection to the Samba LDAP server."""
    server = Server(SAMBA_HOST, port=SAMBA_PORT, get_info=ALL)
    conn = Connection(server, user=SAMBA_ADMIN_DN, password=SAMBA_ADMIN_PASSWORD)
    bound = conn.bind()
    assert bound, f"Failed to bind to LDAP server: {conn.last_error}"
    yield conn
    conn.unbind()


@pytest.fixture
def users_ou(ldap_connection):
    """Create an organizational unit for users."""
    ou_dn = f"ou=users,{SAMBA_BASE_DN}"
    ldap_connection.add(
        ou_dn,
        object_class=["top", "organizationalUnit"],
        attributes={"ou": "users"},
    )
    yield ou_dn
    ldap_connection.delete(ou_dn)


def test_samba_ldap_create_and_read_persons(ldap_connection, users_ou):
    """Test creating persons in Samba LDAP and reading them back."""
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

    created_dns = []
    try:
        for person in persons:
            person_dn = f"uid={person['uid']},{users_ou}"
            result = ldap_connection.add(
                person_dn,
                object_class=["top", "person", "organizationalPerson", "inetOrgPerson"],
                attributes=person,
            )
            assert result, f"Failed to create {person['uid']}: {ldap_connection.result}"
            created_dns.append(person_dn)

        for person in persons:
            ldap_connection.search(
                search_base=users_ou,
                search_filter=f"(uid={person['uid']})",
                attributes=["uid", "cn", "sn", "givenName", "mail"],
            )
            assert len(ldap_connection.entries) == 1, (
                f"Expected 1 entry for {person['uid']}, got {len(ldap_connection.entries)}"
            )

            entry = ldap_connection.entries[0]
            assert str(entry.uid) == person["uid"]
            assert str(entry.cn) == person["cn"]
            assert str(entry.sn) == person["sn"]
            assert str(entry.givenName) == person["givenName"]
            assert str(entry.mail) == person["mail"]

    finally:
        for dn in created_dns:
            ldap_connection.delete(dn)
