# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any

import structlog
from jinja2 import Environment

from .utils import native_value

logger = structlog.stdlib.get_logger()


class LdapConverter:
    def __init__(self, environment: Environment, native_environment: Environment):
        # Renders whole JSON documents, which are parsed by the caller
        self.environment = environment
        # Renders single values, which are returned as native Python types
        self.native_environment = native_environment

    async def render_template(
        self, field_name: str, template_str: str, context: dict[str, Any]
    ) -> Any:
        template = self.native_environment.from_string(template_str)
        # Templates yield the Python object they evaluate to, so '{{ none }}'
        # gives None and '{{ [1, 2] }}' gives a list
        value = native_value(await template.render_async(context))
        logger.debug("Rendered template", field=field_name, value=value)
        return value
