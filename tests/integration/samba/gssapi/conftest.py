# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Fixture overrides for GSSAPI (Kerberos) binds against the Samba AD DC.

Layers on top of the Samba conftest: same server, same Administrator account,
but authenticating with a Kerberos ticket from Samba's built-in KDC instead of
a simple bind.
"""

import subprocess
from configparser import ConfigParser
from pathlib import Path

import pytest

# Kerberos requests a ticket for ldap/<host> of the host we connect to, so this
# must match the DC's dNSHostName (hostname `dc` in realm MAGENTA.DK) and cannot
# be the plain `samba` alias.
SAMBA_HOST = "dc.magenta.dk"
REALM = "MAGENTA.DK"
# Same account the Samba conftest binds with using simple auth
PRINCIPAL = "Administrator"
PASSWORD = "AdminPassword123"


class Krb5Conf(ConfigParser):
    """krb5.conf is an INI-like MIT profile.

    Unlike INI, keys such as realm names are case-sensitive, and realm
    subsections are brace-delimited blocks, which we write as multi-line values.
    """

    def __init__(self) -> None:
        super().__init__(interpolation=None)

    def optionxform(self, optionstr: str) -> str:
        return optionstr


@pytest.fixture
def krb5_client_keytab(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point MIT Kerberos at Samba's KDC and supply a client keytab.

    Production provides credentials through a client keytab that MIT Kerberos
    picks up on its own via KRB5_CLIENT_KTNAME, acquiring and renewing tickets
    without any kinit. We synthesise such a keytab from the known password.
    """
    krb5_conf = Krb5Conf()
    krb5_conf["libdefaults"] = {
        "default_realm": REALM,
        "dns_lookup_kdc": "false",
        "dns_lookup_realm": "false",
        "rdns": "false",
        "dns_canonicalize_hostname": "false",
        "udp_preference_limit": "1",
    }
    krb5_conf["realms"] = {REALM: "\n".join(["{", "kdc = " + SAMBA_HOST, "}"])}
    krb5_conf["domain_realm"] = {"." + REALM.lower(): REALM, REALM.lower(): REALM}
    krb5_conf_path = tmp_path / "krb5.conf"
    with krb5_conf_path.open("w") as fp:
        krb5_conf.write(fp)

    keytab = tmp_path / "client.keytab"
    ktutil_script = [
        f"add_entry -password -p {PRINCIPAL}@{REALM} -k 1 -e aes256-cts-hmac-sha1-96",
        PASSWORD,
        f"write_kt {keytab}",
        "quit",
        "",
    ]
    subprocess.run(
        ["ktutil"],
        input="\n".join(ktutil_script).encode(),
        check=True,
        capture_output=True,
    )

    monkeypatch.setenv("KRB5_CONFIG", str(krb5_conf_path))
    monkeypatch.setenv("KRB5_CLIENT_KTNAME", f"FILE:{keytab}")
    monkeypatch.setenv("KRB5CCNAME", f"FILE:{tmp_path / 'ccache'}")


@pytest.fixture
def integration_test_environment_variables(
    integration_test_environment_variables: None,
    krb5_client_keytab: None,
) -> None:
    """Override: have Kerberos configured before any fixture connects to LDAP."""
