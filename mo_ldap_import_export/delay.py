# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import math
from datetime import datetime
from datetime import time
from zoneinfo import ZoneInfo

from fastapi import HTTPException
from more_itertools import only
from more_itertools import pairwise


async def delay_if_disallowed(disallowed: list[tuple[time, time]]) -> None:
    """Delay processing if we are inside a disallowed interval.

    Args:
        disallowed: The list of disallowed intervals.

    Raises:
        HTTPException: If currently inside a disallowed interval.
            The exception is raised with Retry-After set to when we are allowed to run.
    """
    # Intervals must have start before end
    assert all(start < end for start, end in disallowed)

    # Intervals must be sorted
    assert all(
        l_start < r_start for (l_start, l_end), (r_start, r_end) in pairwise(disallowed)
    )

    # Intervals must not be overlapping, i.e. end of the first must be before start of the second
    assert all(
        l_end < r_start for (l_start, l_end), (r_start, r_end) in pairwise(disallowed)
    )

    # Never disallowed -> always run
    if not disallowed:
        return

    # Find the disallowed interval we are within, if any
    tz = ZoneInfo("Europe/Copenhagen")
    now = datetime.now(tz=tz)

    end_time = only(end for start, end in disallowed if start <= now.time() <= end)
    # If we are not inside a disallowed interval -> run
    if end_time is None:
        return

    # Calculate the number of seconds from now until the end of the interval
    end_dt = datetime.combine(now.date(), end_time, tzinfo=tz)
    next_opening_seconds = (end_dt - now).total_seconds()

    raise HTTPException(
        status_code=503,
        detail="Inside of disallowed interval",
        headers={"Retry-After": str(math.ceil(next_opening_seconds))},
    )
