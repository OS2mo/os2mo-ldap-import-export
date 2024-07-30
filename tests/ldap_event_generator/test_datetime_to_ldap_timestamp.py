# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
import datetime
from datetime import timedelta
from datetime import timezone

import pytest

from mo_ldap_import_export.ldap_event_generator import datetime_to_ldap_timestamp


@pytest.mark.parametrize(
    "datetime,expected",
    [
        (
            datetime.datetime(2021, 1, 1, 10, 45, 20, 0, timezone.utc),
            "20210101104520.000000+0000",
        ),
        (
            datetime.datetime(2021, 1, 1, 10, 45, 20, 2000, timezone.utc),
            "20210101104520.002000+0000",
        ),
        (
            datetime.datetime(2021, 1, 1, 10, 45, 20, 2100, timezone.utc),
            "20210101104520.002100+0000",
        ),
        (
            datetime.datetime(
                2021, 1, 1, 10, 45, 20, 2100, timezone(timedelta(hours=-5, minutes=-29))
            ),
            "20210101104520.002100-0529",
        ),
        # Test sub 4 digit years
        (
            datetime.datetime(1, 1, 1, tzinfo=timezone.utc),
            "00010101000000.000000+0000",
        ),
        (
            datetime.datetime(936, 7, 12, 12, 0, 0, tzinfo=timezone.utc),
            "09360712120000.000000+0000",
        ),
    ],
)
async def test_datetime_to_ldap_timestamp(
    datetime: datetime.datetime, expected: str
) -> None:
    assert datetime_to_ldap_timestamp(datetime) == expected
