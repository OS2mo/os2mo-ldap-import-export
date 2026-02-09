# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Optional


class AttrDict(dict):
    """Enable dot.notation access for a dict object.

    Example:
        ```Python
        script_result = AttrDict({"exit_code": 0})
        assert script_result.exit_code == 0
        ```
    """

    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__  # type: ignore
    __delattr__ = dict.__delitem__  # type: ignore


def attrdict(*args: Any, **kwargs: Optional[Any]) -> AttrDict:
    """Constructor method for `AttrDict`s.

    Example:
        ```Python
        script_result = attrdict({"exit_code": 0})
        assert script_result.exit_code == 0
        ```
    """

    return AttrDict(*args, **kwargs)
