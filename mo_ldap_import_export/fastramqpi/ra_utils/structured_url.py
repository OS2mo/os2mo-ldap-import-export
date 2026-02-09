# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Pydantic StructuredUrl model."""

import json
from typing import Any
from typing import Optional
from urllib.parse import parse_qsl
from urllib.parse import quote
from urllib.parse import urlencode

from pydantic import AnyUrl
from pydantic import BaseModel
from pydantic import Field
from pydantic import SecretStr
from pydantic import parse_obj_as
from pydantic import root_validator


# pylint: disable=too-few-public-methods
class StructuredUrl(BaseModel):
    """Structured Url object.

    Allows for constructing a url either directly or indirectly."""

    class Config:
        """Settings are frozen."""

        frozen = True

    url: AnyUrl = Field(..., description="Database URL.")

    scheme: str | None
    user: str | None
    password: SecretStr | None
    host: str | None
    port: int | None
    path: str | None
    query: dict[str, str] | None
    fragment: str | None

    @root_validator(pre=True)
    def ensure_url(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Ensure that url is set.

        Args:
            values: Pydantic parsed values.

        Returns:
            'values' but with the guarantee that 'url' is set.
        """
        # If 'url' is set, noop.
        if values.get("url"):
            structured_field_keys = (
                "scheme",
                "user",
                "password",
                "host",
                "port",
                "path",
                "query",
                "fragment",
            )
            if not values.keys().isdisjoint(structured_field_keys):
                raise ValueError("cannot provide both url and structured fields")
            return values

        if "scheme" not in values:
            raise ValueError("scheme is required")
        if "host" not in values:
            raise ValueError("host is required")

        # Ensure that query is a dictionary or None
        query = values.get("query", {})
        if isinstance(query, str):
            query = json.loads(query)

        # According to RFC3986 section 3.2.1
        # Basic-auth username and password should be URL encoded
        user = values.get("user")
        if user:
            user = quote(user)
        password = values.get("password")
        if password:
            password = quote(password)

        uri_string = AnyUrl.build(
            scheme=values.get("scheme"),
            user=user,
            password=password,
            host=values.get("host"),
            port=parse_obj_as(Optional[str], values.get("port")),  # type: ignore[arg-type]
            path=values.get("path"),
            query=urlencode(query),
            fragment=values.get("fragment"),
        )
        values["url"] = parse_obj_as(AnyUrl, uri_string)
        return values

    @root_validator(pre=True)
    def ensure_xstructured_fields(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Ensure that our structured fields are set.

        Args:
            values: Pydantic parsed values.

        Returns:
            'values' but with the guarantee that all non-'url' fields are set.
        """
        # If 'url' is not set at this point, error out.
        assert values["url"] is not None

        url = parse_obj_as(AnyUrl, values["url"])
        values.update(
            dict(
                scheme=url.scheme,
                user=url.user,
                password=url.password,
                host=url.host,
                port=parse_obj_as(Optional[int], url.port),  # type: ignore[arg-type]
                path=url.path,
                query=dict(parse_qsl(url.query)),
                fragment=url.fragment,
            )
        )
        return values
