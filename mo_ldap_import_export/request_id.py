# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Forward the current request_id to MO as the X-Request-ID header."""

import httpx
import structlog


async def _forward_request_id(request: httpx.Request) -> None:
    request_id = structlog.contextvars.get_contextvars().get("request_id")
    if request_id is not None:
        request.headers["x-request-id"] = request_id


def install_request_id_forwarding(http_client: httpx.AsyncClient) -> None:
    """Register the X-Request-ID forwarding hook on the httpx client."""
    http_client.event_hooks["request"].append(_forward_request_id)
