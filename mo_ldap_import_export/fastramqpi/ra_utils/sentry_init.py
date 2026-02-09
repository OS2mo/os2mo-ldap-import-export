# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import List
from typing import Optional

import sentry_sdk
import structlog
from pydantic import BaseSettings
from pydantic import HttpUrl
from pydantic import ValidationError


class Settings(BaseSettings):
    dsn: HttpUrl

    release: Optional[str]
    environment: Optional[str]
    server_name: Optional[str]
    traces_sample_rate: Optional[str]
    max_breadcrumbs: Optional[str]
    debug: Optional[bool]
    attach_stacktrace: Optional[bool]
    integrations: Optional[List[str]]
    # TODO: Add more settings as needed

    class Config:
        env_prefix = "SENTRY_"


def sentry_init(*args: Any, **kwargs: Any) -> bool:
    """Initialize sentry
    Configure with inputs as kwargs or environment variables
    prefixed with SENTRY_
    Only sets up Sentry if sentry_dsn is found.
    Returns True if sentry was initialized and False if not.
    """
    log = structlog.get_logger()
    try:
        settings = Settings(*args, **kwargs)
    except ValidationError:
        log.info("Sentry init skipped")
        return False
    log.info(f"Setting up Sentry with {settings=}")
    sentry_sdk.init(**settings.dict())
    return True
