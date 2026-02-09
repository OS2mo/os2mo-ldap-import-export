# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import traceback
from uuid import uuid4

import structlog
from fastapi.responses import PlainTextResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.base import RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from structlog.contextvars import bound_contextvars

logger = structlog.stdlib.get_logger()


class RequestIdMiddleware(BaseHTTPMiddleware):
    """Bind incoming X-Request-ID header to Structlog."""

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        request_id = request.headers.get("x-request-id", str(uuid4()))
        with bound_contextvars(request_id=request_id):
            return await call_next(request)


class ExceptionMiddleware(BaseHTTPMiddleware):
    """Properly log and return exceptions.

    Uncaught exceptions are normally logged by uvicorn, but that doesn't
    include our request id and other bound contextvars. Explicitly log
    exceptions and return the full traceback as the HTTP response.
    """

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        try:
            return await call_next(request)
        except Exception:
            logger.exception("Exception in application")
            return PlainTextResponse(content=traceback.format_exc(), status_code=500)
