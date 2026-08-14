# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Tests for the native rendering of ldap2mo field templates.

Field templates yield the Python object that the template evaluates to, rather
than a string that has to be guessed at afterwards.
"""

from typing import Any
from uuid import UUID

import pytest
from jinja2.nativetypes import NativeEnvironment

from mo_ldap_import_export.converters import LdapConverter
from mo_ldap_import_export.environments.main import construct_default_environment


@pytest.fixture
def converter() -> LdapConverter:
    return LdapConverter(
        construct_default_environment(),
        construct_default_environment(NativeEnvironment),
    )


@pytest.mark.parametrize(
    "template,expected",
    (
        # Containers are rendered as containers, not as their repr
        ("{{ [1, 2] }}", [1, 2]),
        ("{{ ['a', 'b'] }}", ["a", "b"]),
        ("{{ {'a': 1} }}", {"a": 1}),
        ("{{ [] }}", []),
        # As are the other native types
        ("{{ none }}", None),
        ("{{ 1 == 1 }}", True),
        ("{{ 1 == 2 }}", False),
        ("{{ 42 }}", 42),
        ("{{ uuid4() is not none }}", True),
        # Strings that look like a number are numbers
        ("12345678", 12345678),
        # ... unless they cannot be, f.x. because of a leading zero
        ("0101700001", "0101700001"),
        ("2024-01-01", "2024-01-01"),
        ("+45 12 34 56 78", "+45 12 34 56 78"),
        # Insignificant whitespace is stripped from strings
        ("  foo  ", "foo"),
        ("\n  {{ 'foo' }}\n  ", "foo"),
        # An indented multi-line template yields the same value as a compact one
        ("\n    {{\n        none\n    }}\n    ", None),
        ("\n    {{\n        [1, 2]\n    }}\n    ", [1, 2]),
        ("{% if true %}\n    {{ none }}\n{% endif %}", None),
        # An empty template yields no value at all
        ("", None),
        # JSON text is not a Python literal, so it stays a string.
        # Templates must yield the object itself instead, see the test below.
        ('{"a": null}', '{"a": null}'),
    ),
)
async def test_render_template(
    converter: LdapConverter, template: str, expected: Any
) -> None:
    value = await converter.render_template("field", template, {})
    assert value == expected
    assert type(value) is type(expected)


async def test_render_template_yields_objects(converter: LdapConverter) -> None:
    """Templates can yield arbitrary Python objects, not just literals."""
    uuid = UUID("fa15edad-da1e-c0de-babe-c1a551f1ab1e")
    value = await converter.render_template("field", "{{ uuid }}", {"uuid": uuid})
    assert value is uuid


async def test_render_template_list_of_objects(converter: LdapConverter) -> None:
    """The '_for_each_' idiom of collecting objects into a list."""
    template = """
        {%- set collected = [] -%}
        {%- for value in values -%}
            {%- set _ = collected.append(value|uuid) -%}
        {%- endfor -%}
        {{- collected -}}
    """
    uuids = [
        "fa15edad-da1e-c0de-babe-c1a551f1ab1e",
        "c0ffee00-dead-beef-cafe-f00dbaaaaaad",
    ]
    value = await converter.render_template("field", template, {"values": uuids})
    assert value == [UUID(uuid) for uuid in uuids]


async def test_render_template_undefined_variable(converter: LdapConverter) -> None:
    with pytest.raises(Exception) as exc_info:
        await converter.render_template("field", "{{ __undefined__ }}", {})
    assert "Undefined variable '__undefined__'" in str(exc_info.value)
