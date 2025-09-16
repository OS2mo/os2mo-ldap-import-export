# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from mo_ldap_import_export.environments.main import org_unit_path_string_from_dn


def test_org_unit_path_string_from_dn() -> None:
    dn = "CN=Angus,OU=Auchtertool,OU=Kingdom of Fife,OU=Scotland,DC=gh"

    org_unit_path = org_unit_path_string_from_dn("\\", dn)
    assert org_unit_path == "Scotland\\Kingdom of Fife\\Auchtertool"

    org_unit_path = org_unit_path_string_from_dn("\\", dn, 1)
    assert org_unit_path == "Kingdom of Fife\\Auchtertool"

    org_unit_path = org_unit_path_string_from_dn("\\", dn, 2)
    assert org_unit_path == "Auchtertool"

    org_unit_path = org_unit_path_string_from_dn("\\", dn, 3)
    assert org_unit_path == ""
