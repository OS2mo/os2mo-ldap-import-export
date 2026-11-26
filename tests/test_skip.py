# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import pytest

from mo_ldap_import_export.environments.main import construct_default_environment
from mo_ldap_import_export.exceptions import SkipObject


@pytest.mark.parametrize(
    "template,expected",
    [
        ("{{ skip() }}", "Skipping: Explicitly skipped"),
        ("{{ skip('No account') }}", "No account"),
        ("{% if true %}{{ skip() }}{% endif %}", "Skipping: Explicitly skipped"),
    ],
)
async def test_skip(template: str, expected: str) -> None:
    environment = construct_default_environment()
    with pytest.raises(SkipObject) as exc_info:
        await environment.from_string(template).render_async()
    assert str(exc_info.value) == expected


async def test_skip_not_reached() -> None:
    environment = construct_default_environment()
    template = environment.from_string("{% if false %}{{ skip() }}{% endif %}ok")
    assert await template.render_async() == "ok"
