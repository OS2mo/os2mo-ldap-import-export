# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import pytest
from ldap3.core.exceptions import LDAPInvalidDnError

from mo_ldap_import_export.environments.main import org_unit_path_from_dn
from mo_ldap_import_export.types import DN


@pytest.mark.parametrize(
    "dn",
    [
        "",
        "DC=com",
        "DC=example,DC=com",
        "CN=Jim,DC=example,DC=com",
        "O=magenta,DC=example,DC=com",
        "CN=Jim,O=magenta,DC=example,DC=com",
    ],
)
def test_org_unit_path_from_dn_no_dn(dn: DN) -> None:
    with pytest.raises(LDAPInvalidDnError) as exc_info:
        org_unit_path_from_dn(dn)
    assert "empty dn" in str(exc_info.value)


@pytest.mark.parametrize(
    "dn,expected",
    [
        (
            "OU=A",
            ["A"],
        ),
        (
            "OU=A,OU=B",
            ["B", "A"],
        ),
        (
            "OU=A,OU=B,OU=C",
            ["C", "B", "A"],
        ),
        (
            "CN=Jim,OU=A,OU=B,OU=C,O=org,DC=example,DC=COM",
            ["C", "B", "A"],
        ),
        (
            "CN=Angus,OU=Auchtertool,OU=Kingdom of Fife,OU=Scotland,DC=gh",
            ["Scotland", "Kingdom of Fife", "Auchtertool"],
        ),
        (
            "CN=Jim,OU=Technicians,OU=Users,OU=demo,OU=OS2MO,DC=ad,DC=addev",
            ["OS2MO", "demo", "Users", "Technicians"],
        ),
    ],
)
def test_org_unit_path_from_dn(dn: DN, expected: list[str]) -> None:
    org_unit_path = org_unit_path_from_dn(dn)
    assert org_unit_path == expected
