# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import pytest

from mo_ldap_import_export.ldap import evaluate_template
from mo_ldap_import_export.types import DN


@pytest.mark.parametrize(
    "template,expected",
    (
        # Templates are rendered to native Python types, so comparisons yield booleans
        ("{{ value == 'foo' }}", True),
        ("{{ value == 'bar' }}", False),
        ("{{ 'CN=foo' in dn }}", True),
        ("{{ sn == 'foo' }}", True),
        # Whitespace around the rendered value is insignificant
        ("\n{{ value == 'foo' }}\n", True),
        ("\n    {% set match = value == 'foo' %}\n    {{ match }}\n    ", True),
        ("\n    {% set match = value == 'bar' %}\n    {{ match }}\n    ", False),
        # Boolean literals are booleans too
        ("True", True),
        ("False", False),
        # Anything that does not evaluate to 'True' is not a match
        ("true", False),
        ("__never_gonna_match__", False),
        ("{{ value }}", False),
        ("{{ 1 }}", False),
        ("{{ none }}", False),
    ),
)
async def test_evaluate_template(template: str, expected: bool) -> None:
    result = await evaluate_template(
        template, dn=DN("CN=foo,o=test"), mapping={"sn": "foo"}
    )
    assert result is expected


async def test_evaluate_template_undefined_variable() -> None:
    with pytest.raises(Exception) as exc_info:
        await evaluate_template(
            "{{ __undefined__ }}", dn=DN("CN=foo,o=test"), mapping={}
        )
    assert "Undefined variable '__undefined__'" in str(exc_info.value)
