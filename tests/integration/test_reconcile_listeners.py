# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Tests the gating of the reconciliation listeners."""

import pytest

from mo_ldap_import_export.config import Settings
from mo_ldap_import_export.depends import GraphQLClient


@pytest.mark.integration_test
@pytest.mark.parametrize(
    "",
    [
        pytest.param(
            id="both",
            marks=[
                pytest.mark.envvar(
                    {
                        "LISTEN_TO_CHANGES_IN_MO": "True",
                        "LISTEN_TO_CHANGES_IN_LDAP": "True",
                    },
                ),
            ],
        ),
        pytest.param(
            id="mo_only",
            marks=[
                pytest.mark.envvar(
                    {
                        "LISTEN_TO_CHANGES_IN_MO": "True",
                        "LISTEN_TO_CHANGES_IN_LDAP": "False",
                    },
                ),
                pytest.mark.xfail(
                    strict=True,
                    reason="gated by its own namespace, not the mapping it protects",
                ),
            ],
        ),
        pytest.param(
            id="ldap_only",
            marks=[
                pytest.mark.envvar(
                    {
                        "LISTEN_TO_CHANGES_IN_MO": "False",
                        "LISTEN_TO_CHANGES_IN_LDAP": "True",
                    },
                ),
                pytest.mark.xfail(
                    strict=True,
                    reason="gated by its own namespace, not the mapping it protects",
                ),
            ],
        ),
        pytest.param(
            id="neither",
            marks=[
                pytest.mark.envvar(
                    {
                        "LISTEN_TO_CHANGES_IN_MO": "False",
                        "LISTEN_TO_CHANGES_IN_LDAP": "False",
                    },
                ),
            ],
        ),
    ],
)
@pytest.mark.usefixtures("test_client")
async def test_reconcile_guards_the_mapping_it_protects(
    graphql_client: GraphQLClient,
) -> None:
    """The registered reconciles are checked against the listen toggles.

    Listeners are declared at startup and read back from MO.
    """
    settings = Settings()
    listeners = await graphql_client.read_event_listeners()
    user_keys = {listener.user_key for listener in listeners.objects}

    ldap2mo_reconcile = "internal_reconcile_uuid"
    mo2ldap_reconcile = f"{settings.event_namespace}_internal_reconcile_person"

    assert (ldap2mo_reconcile in user_keys) == settings.listen_to_changes_in_mo
    assert (mo2ldap_reconcile in user_keys) == settings.listen_to_changes_in_ldap
