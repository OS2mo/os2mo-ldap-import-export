# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Integration tests for LDAPS against the OpenLDAP."""

from textwrap import dedent

import pytest
from ldap3 import Connection
from ldap3.core.exceptions import LDAPSocketOpenError

from mo_ldap_import_export.config import ServerConfig
from mo_ldap_import_export.ldap import construct_server

# Issued for 'openldap.magenta.dk', not 'ldap' which the service is reached on.
# Must match TLS_CERT in docker-compose.yml and .gitlab-ci.yml
CERTIFICATE = dedent("""\
    -----BEGIN CERTIFICATE-----
    MIIBszCCAVmgAwIBAgIUQweuM6DA3SfaMse1MIyeIA2EkrEwCgYIKoZIzj0EAwIw
    HjEcMBoGA1UEAwwTb3BlbmxkYXAubWFnZW50YS5kazAgFw0yNjEwMDYxNTE3NTFa
    GA8yMTI2MDkxMjE1MTc1MVowHjEcMBoGA1UEAwwTb3BlbmxkYXAubWFnZW50YS5k
    azBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABGSxB2IpQ4oWcttfGcbIwulZR56d
    TAm+KA87ayunpGw+ug75+uvphwD+zaxt+b5uPp5DTpfWfknOb+WPtNcZA0GjczBx
    MB0GA1UdDgQWBBRfqFIYYU+QyVq1d63D8hMaGBnsRTAfBgNVHSMEGDAWgBRfqFIY
    YU+QyVq1d63D8hMaGBnsRTAPBgNVHRMBAf8EBTADAQH/MB4GA1UdEQQXMBWCE29w
    ZW5sZGFwLm1hZ2VudGEuZGswCgYIKoZIzj0EAwIDSAAwRQIgXIv6RhWl0yU+xShe
    tuWNAvEUpI+GWDjbQoQP0ugrW0ICIQDYF47EH49fnrVVHi4Bx3NOH2ddRrFZcho5
    8ay+9tGopA==
    -----END CERTIFICATE-----
    """)
# Must match LDAPS_PORT in docker-compose.yml and .gitlab-ci.yml
PORT = 2636


@pytest.mark.integration_test
@pytest.mark.parametrize(
    "ca_certs_data,expected",
    [
        # Unpinned, so the certificate is checked against the system CAs,
        # which do not trust it
        (None, "certificate verify failed: self-signed certificate"),
        # Pinned to the certificate itself, leaving hostname verification to fail
        (
            CERTIFICATE,
            "'subjectAltName': (('DNS', 'openldap.magenta.dk'),)}"
            " doesn't match any name in ['ldap']",
        ),
    ],
)
def test_ldaps_rejects_the_certificate(
    ca_certs_data: str | None, expected: str
) -> None:
    """Verifying the certificate fails, as it does not name 'ldap'."""
    server = construct_server(
        ServerConfig(host="ldap", port=PORT, use_ssl=True, ca_certs_data=ca_certs_data)
    )
    connection = Connection(server)

    with pytest.raises(LDAPSocketOpenError) as exc_info:
        connection.open(read_server_info=False)

    assert expected in str(exc_info.value)


@pytest.mark.integration_test
@pytest.mark.parametrize("ca_certs_data", [None, CERTIFICATE])
def test_ldaps_insecure_ignores_the_certificate(ca_certs_data: str | None) -> None:
    """LDAPS connects when verification is disabled, pinned or not."""
    server = construct_server(
        ServerConfig(
            host="ldap",
            port=PORT,
            use_ssl=True,
            ca_certs_data=ca_certs_data,
            insecure=True,
        )
    )
    connection = Connection(server)

    connection.open(read_server_info=False)

    connection.socket.close()
