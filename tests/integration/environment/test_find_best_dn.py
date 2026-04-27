# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json
from typing import cast

import pytest
from fastramqpi.context import Context
from httpx import AsyncClient

from mo_ldap_import_export.dataloaders import DataLoader
from mo_ldap_import_export.environments.main import find_best_dn
from mo_ldap_import_export.exceptions import MultipleObjectsReturnedException
from mo_ldap_import_export.utils import combine_dn_strings
from tests.integration.conftest import AddLdapPerson

# CPR matching the `mo_person` fixture in tests/integration/conftest.py
MO_PERSON_CPR = "2108613133"


@pytest.fixture
def dataloader(test_client: AsyncClient, context: Context) -> DataLoader:
    return cast(DataLoader, context["user_context"]["dataloader"])


@pytest.mark.integration_test
@pytest.mark.usefixtures("mo_person")
@pytest.mark.envvar(
    {
        "DISCRIMINATOR_FIELDS": json.dumps(["uid"]),
        "DISCRIMINATOR_VALUES": json.dumps(
            [
                "{{ uid|length == 3 }}",
                "{{ uid|length == 4 }}",
                "{{ uid|length >= 5 }}",
            ]
        ),
    }
)
async def test_find_best_dn_picks_best_account_for_person(
    dataloader: DataLoader,
    add_ldap_person: AddLdapPerson,
) -> None:
    """All accounts share a CPR, the best (shortest uid) wins."""
    ava = combine_dn_strings(await add_ldap_person("ava", MO_PERSON_CPR))
    cleo = combine_dn_strings(await add_ldap_person("cleo", MO_PERSON_CPR))
    emily = combine_dn_strings(await add_ldap_person("emily", MO_PERSON_CPR))

    for dn in (ava, cleo, emily):
        result = await find_best_dn(dataloader, dn)
        assert result == ava


@pytest.mark.integration_test
@pytest.mark.usefixtures("mo_person")
@pytest.mark.envvar(
    {
        "DISCRIMINATOR_FIELDS": json.dumps(["uid"]),
        "DISCRIMINATOR_VALUES": json.dumps(["{{ 'ass' not in dn }}"]),
        "DISCRIMINATOR_FILTER": "{{ 'class' not in dn }}",
    }
)
async def test_find_best_dn_runs_filter_and_apply(
    dataloader: DataLoader,
    add_ldap_person: AddLdapPerson,
) -> None:
    """All accounts share a CPR, but only `ava` survives both filter and apply."""
    # Allowed by both filter and discriminator
    ava = combine_dn_strings(await add_ldap_person("ava", MO_PERSON_CPR))
    # Removed by filter (contains "class")
    classic = combine_dn_strings(await add_ldap_person("classic", MO_PERSON_CPR))
    # Removed by discriminator (contains "ass")
    grass = combine_dn_strings(await add_ldap_person("grass", MO_PERSON_CPR))

    for dn in (ava, classic, grass):
        result = await find_best_dn(dataloader, dn)
        assert result == ava


@pytest.mark.integration_test
@pytest.mark.usefixtures("mo_person")
@pytest.mark.envvar(
    {
        "DISCRIMINATOR_FIELDS": json.dumps(["uid"]),
        "DISCRIMINATOR_VALUES": json.dumps(["{{ 'ass' not in dn }}"]),
    }
)
async def test_find_best_dn_returns_none_when_all_rejected(
    dataloader: DataLoader,
    add_ldap_person: AddLdapPerson,
) -> None:
    """All accounts share a CPR, but all are rejected and we get None."""
    classic = combine_dn_strings(await add_ldap_person("classic", MO_PERSON_CPR))
    grass = combine_dn_strings(await add_ldap_person("grass", MO_PERSON_CPR))

    for dn in (classic, grass):
        result = await find_best_dn(dataloader, dn)
        assert result is None


@pytest.mark.integration_test
@pytest.mark.usefixtures("mo_person")
@pytest.mark.envvar(
    {
        "DISCRIMINATOR_FIELDS": json.dumps(["uid"]),
        "DISCRIMINATOR_VALUES": json.dumps(["True"]),
    }
)
async def test_find_best_dn_unrelated_dn_not_considered(
    dataloader: DataLoader,
    add_ldap_person: AddLdapPerson,
) -> None:
    """Accounts with a different CPR don't belong to the same MO person."""
    a = combine_dn_strings(await add_ldap_person("alice", MO_PERSON_CPR))
    # Different CPR -> not associated with mo_person -> not a candidate
    await add_ldap_person("bob", "0101700001")

    result = await find_best_dn(dataloader, a)
    assert result == a


@pytest.mark.integration_test
async def test_find_best_dn_no_mo_person(
    dataloader: DataLoader,
    add_ldap_person: AddLdapPerson,
) -> None:
    """A DN with no associated MO person returns None."""
    dn = combine_dn_strings(await add_ldap_person("orphan", "0101700099"))

    result = await find_best_dn(dataloader, dn)
    assert result is None


@pytest.mark.integration_test
@pytest.mark.usefixtures("mo_person")
@pytest.mark.envvar(
    {
        "DISCRIMINATOR_FIELDS": json.dumps(["uid"]),
        "DISCRIMINATOR_VALUES": json.dumps(["True"]),
    }
)
async def test_find_best_dn_not_unique(
    dataloader: DataLoader,
    add_ldap_person: AddLdapPerson,
) -> None:
    """All ccounts share a CPR, and all pass discriminator, result is ambiguous."""
    classic = combine_dn_strings(await add_ldap_person("classic", MO_PERSON_CPR))
    await add_ldap_person("grass", MO_PERSON_CPR)

    with pytest.raises(MultipleObjectsReturnedException) as exc_info:
        await find_best_dn(dataloader, classic)
    assert "Ambiguous account result from apply discriminator" in str(exc_info.value)
