# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Common interface for LDAP event generators."""

from typing import Any
from typing import Protocol
from typing import Self

from fastramqpi.context import Context

from .types import LDAPUUID


class LDAPEventGeneratorProtocol(Protocol):
    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self, __exc_type: object, __exc_value: object, __traceback: object
    ) -> None: ...
    async def healthcheck(self, context: dict | Context) -> bool: ...
    async def poll(self, state: Any) -> tuple[set[LDAPUUID], Any]: ...
    def encode_poll_state(self, state: Any) -> str: ...
    def decode_poll_state(self, token: str | None) -> Any: ...
