# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from fastapi import Depends
from fastapi import FastAPI
from fastapi.testclient import TestClient
from freezegun import freeze_time

from mo_ldap_import_export.config import Settings
from mo_ldap_import_export.ldap_amqp import delay_ldap


@pytest.mark.parametrize(
    "date,feature_flag,status_code,retry_after",
    [
        # Outside the disallowed interval
        ("2025-01-01T12:00:00+01:00", False, 200, None),
        ("2025-01-01T12:00:00+01:00", True, 200, None),
        # Inside the disallowed interval
        ("2025-01-01T08:00:00+01:00", False, 200, None),
        ("2025-01-01T08:00:00+01:00", True, 503, str(45 * 60)),
        # Various freeze times
        ("2025-01-01T08:00:01+01:00", True, 503, str(45 * 60 - 1)),
        ("2025-01-01T08:44:30+01:00", True, 503, "30"),
        # Border condition
        ("2025-01-01T07:45:00+01:00", True, 503, str(60 * 60)),
        ("2025-01-01T08:45:00+01:00", True, 200, None),
    ],
)
@pytest.mark.usefixtures("minimal_valid_environmental_variables")
async def test_delay_feature_flag(
    date: str,
    feature_flag: bool,
    status_code: int,
    retry_after: str | None,
) -> None:
    """Tests the delay_ldap dependency in isolation."""
    settings = Settings(disallow_ldap_processing_between_7_45_and_8_45=feature_flag)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[dict]:
        yield {"context": {"user_context": {"settings": settings}}}

    app = FastAPI(lifespan=lifespan)

    @app.post("/endpoint", dependencies=[Depends(delay_ldap)])
    async def endpoint() -> None:
        return

    with TestClient(app) as client, freeze_time(date):
        r = client.post("/endpoint")
        assert r.status_code == status_code
        assert r.headers.get("Retry-After") == retry_after
