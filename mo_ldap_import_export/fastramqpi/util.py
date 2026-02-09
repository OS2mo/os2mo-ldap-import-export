# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Never


class TerminateTaskGroup(Exception):
    pass


async def terminate_task_group() -> Never:
    """Used to force termination of a task group.

    https://docs.python.org/3/library/asyncio-task.html#terminating-a-task-group
    """
    raise TerminateTaskGroup()
