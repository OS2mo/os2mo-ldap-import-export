# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import ast
from datetime import datetime
from importlib.metadata import version
from typing import Any
from typing import cast
from zoneinfo import ZoneInfo

from ariadne_codegen.plugins.base import Plugin
from graphql import GraphQLInputField
from graphql import Undefined
from pydantic.datetime_parse import StrBytesIntFloat
from pydantic.datetime_parse import parse_datetime

MO_TZ = ZoneInfo("Europe/Copenhagen")


def parse_graphql_datetime(value: StrBytesIntFloat | datetime) -> datetime:
    """Parse OS2mo GraphQL datetime to Python object.

    Even though ISO 8601 promises "unique and unambiguous" representations of
    datetimes, it is impossible to unambiguously represent timestamp in a
    specific time zone using only a UTC offset.

    For example, the 31st October 2010 at 00:00 in Copenhagen and Cairo is both
    represented as the timestamp `2010-10-31T00:00:00+02:00`. However, one day
    later in Copenhagen is represented as `2010-11-01T00:00:00+01:00` -- now
    with a UTC offset of 01:00 because of daylight saving time -- whereas the
    timestamp in Cairo is `2010-11-01T00:00:00+02:00`.

    ISO 8601 is thus inherently a broken standard. Until RFC 9557 introduces
    proper timestamps with time zone locations, the best we can do is assume
    that everyone observes Copenhagen time.
    """
    # Datetime objects can have proper time zone information, so we refrain
    # from modifying it. Values received from OS2mo will always be strings.
    if isinstance(value, datetime):
        return value
    dt = parse_datetime(value)
    # Only assume Copenhagen time if UTC offset matches Copenhagen
    if dt.utcoffset() != dt.astimezone(MO_TZ).utcoffset():
        return dt
    return dt.replace(tzinfo=MO_TZ)


def _is_ast_annotation_optional(annotation: ast.expr) -> bool:
    match annotation:
        # This case handles the `B[A]` syntax
        case ast.Subscript():
            # To be optional, `B` in the syntax must be the string "Optional"
            value = annotation.value
            # Not a name --> not a match
            if not isinstance(value, ast.Name):
                return False
            return value.id == "Optional"

        # This case handles the `A op B` syntax
        case ast.BinOp():
            # To be optional, `B` in the syntax must be None and op must be "|"
            op = annotation.op
            # if op is not "|" this is not a match
            if not isinstance(op, ast.BitOr):
                return False
            match annotation.right:
                case ast.Name():
                    return annotation.right.id == "None"
                case ast.Constant():
                    return annotation.right.value is None
                case _:
                    return False

        # This case handles the stringified `"Optional[A]"` syntax
        # This case handles the stringified `"A | None"` syntax
        case ast.Name():
            return "Optional[" in annotation.id or "| None" in annotation.id

        # This case handles the stringified `"Optional[A]"` syntax
        # This case handles the stringified `"A | None"` syntax
        case ast.Constant():
            return "Optional[" in annotation.value or "| None" in annotation.value

        case _:
            return False


class UnsetInputTypesPlugin(Plugin):
    """Ariadne plugin to handle Strawberry UNSET types in input types.

    Version 0.7.1 of Ariadne codegen handles UNSET types nicely for arguments in
    queries, but not within input fields, thus fields that should have been
    optional and defaulting to UNSET are instead optional, but required due to
    having no default value.

    It is not possible to simply set the value to UNSET when constructing the
    objects as Pydantic refuses to parse the UNSET variable as an optional
    Employee or similar.

    Future versions of Ariadne seemingly handles UNSET in input types correctly,
    by utilizing the `exclude_unset` feature in Pydantic 2, however we are bound
    to Pydantic 1, as MO cannot be upgraded due to FastAPI being a blocker.

    Thus for now we are at an impasse that this plugin attempts to solve.

    The plugin modifies the generated GraphQL client, such that input types which
    are UNSET in the GraphQL schema can be either their original type or the
    UnsetType while defaulting to the UNSET type.

    This ensures that the fields are no longer required, and that the UNSET
    behavior works as expected, as Ariadne 0.7.1 correctly strips UNSET values
    from the payloads within the `execute` method of the client, as used by
    arguments in queries.

    See:
    * https://github.com/mirumee/ariadne-codegen/issues/121
    * https://github.com/mirumee/ariadne-codegen/pull/128
    * https://github.com/mirumee/ariadne-codegen/issues/165
    For details.

    The plugin can be used by inserting it into the `tool.ariadne-codgen` section
    of the `pyproject.toml` file, as shown below:
    ```toml
        [tool.ariadne-codegen]
        target_package_name = "autogenerated_graphql_client"
        ...
        plugins = [
            # Return values directly when only a single top field is requested
            "ariadne_codegen.contrib.shorter_results.ShorterResultsPlugin",
            # Add UNSET and UnsetType to generated input types
            "fastramqpi.ariadne.UnsetInputTypesPlugin",  # <-- This
        ]
    ```

    This plugin can be removed once we utilize a newer version of Ariadne codegen.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # This plugin probably does not work on pydantic v2 / newer ariadne versions.
        if version("ariadne-codegen") != "0.7.1":  # pragma: no cover
            raise AssertionError(
                "The UnsetInputTypesPlugin only works on Ariadne 0.7.1"
            )

    def generate_inputs_module(self, module: ast.Module) -> ast.Module:
        # This imports the UnsetType and UNSET value atop the input_types module
        unset_imports = [
            ast.ImportFrom(
                level=1, module="base_model", names=[ast.alias("UnsetType")]
            ),
            ast.ImportFrom(level=1, module="base_model", names=[ast.alias("UNSET")]),
        ]

        module.body = unset_imports + module.body
        return module

    def generate_input_field(
        self,
        field_implementation: ast.AnnAssign,
        input_field: GraphQLInputField,
        field_name: str,
    ) -> ast.AnnAssign:
        # Only fields without default values in the GraphQL schema default to UNSET
        if input_field.default_value != Undefined:
            return field_implementation

        # Only optional fields can have difference between UNSET and null
        optional = _is_ast_annotation_optional(field_implementation.annotation)
        if not optional:
            return field_implementation

        # This occurs for fields generated with aliasses, such as `from` being
        # generated as `_from: datetime = Field(alias="from")`, thus being given
        # a default value even if one does not exist in the GraphQL schema.
        # We intentionally ignore these cases, as it only really affects `from`
        # and as we are in the process of replacing `from`/`to` with `start`/`end`.
        if field_implementation.value is not None:
            return field_implementation

        new_annotation: ast.expr
        # Types in the schema may either be real types or stringified ones
        # i.e. either `Optional[MyType]` or `"Optional[MyType]"`.
        # The below code has a branch for each case.
        if (
            isinstance(field_implementation.annotation, ast.Name)
            and '"' in field_implementation.annotation.id
        ) or (isinstance(field_implementation.annotation, ast.Constant)):
            # This handles the stringified case, by stripping the quotes,
            # adding our new type and readding the quotes, such that
            # `"Optional[MyType]"` becomes `"Optional[MyType] | UnsetType"`.
            annotation = field_implementation.annotation
            annotation_value: str = cast(
                str,
                (
                    annotation.id
                    if isinstance(annotation, ast.Name)
                    else annotation.value
                ),
            )
            unquoted_id = annotation_value.strip('"')
            new_annotation = ast.Name(f'"{unquoted_id} | UnsetType"')
        else:
            # This handles real types, by introducing a BinOp.
            # The BinOp ast node is; `left OP right`, where:
            # * `left` is the old annotation,
            # * `op` is `BitOr` aka `|` and
            # * `b` is our `UnsetType`
            # Such that `Optional[MyType]` becomes `Optional[MyType] | UnsetType`.
            new_annotation = ast.BinOp(
                left=field_implementation.annotation,
                op=ast.BitOr(),
                right=ast.Name("UnsetType"),
            )

        # This creates an assignment ast node, i.e. `target: annotation = value`,
        # where we update the annotation to the newly calculated one, and the value
        # is set to `UNSET`.
        return ast.AnnAssign(
            target=field_implementation.target,
            annotation=new_annotation,
            value=ast.Name("UNSET"),
            simple=field_implementation.simple,
        )

    def generate_inputs_code(self, generated_code: str) -> str:
        plugin_header = "# This file has been modified by the UnsetInputTypesPlugin\n"
        return plugin_header + generated_code
