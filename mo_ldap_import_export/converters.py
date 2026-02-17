# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json
from json.decoder import JSONDecodeError
from typing import Any

import structlog
from jinja2 import Environment

from .exceptions import IncorrectMapping

logger = structlog.stdlib.get_logger()


class LdapConverter:
    def __init__(self, environment: Environment) -> None:
        self.environment = environment

    @staticmethod
    def str_to_dict(text):
        """
        Converts a string to a dictionary
        """
        return json.loads(text.replace("'", '"').replace("Undefined", "null"))

    async def render_template(
        self, field_name: str, template_str: str, context: dict[str, Any]
    ) -> Any:
        template = self.environment.from_string(template_str)
        value = (await template.render_async(context)).strip()

        # Sloppy mapping can lead to the following rendered strings:
        # - {{ldap.mail or None}} renders as "None"
        # - {{ldap.mail}} renders as "[]" if ldap.mail is empty
        #
        # Mapping with {{ldap.mail or ''}} solves both, but let's check
        # for "none" or "[]" strings anyway to be more robust.
        if value.lower() == "none":
            return None
        if value == "[]":
            return []

        # TODO: Is it possible to render a dictionary directly?
        #       Instead of converting from a string
        looks_like_a_dict = "{" in value and ":" in value and "}" in value
        looks_like_a_list = value.startswith("[") and value.endswith("]")
        if looks_like_a_dict or looks_like_a_list:
            try:
                value = self.str_to_dict(value)
            except JSONDecodeError as error:
                error_string = f"Could not convert {value} in '{field_name}' to dict (context={context!r})"
                raise IncorrectMapping(error_string) from error
        return value
