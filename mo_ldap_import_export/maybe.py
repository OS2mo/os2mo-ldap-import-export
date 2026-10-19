# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Maybe monad inspired by strawberry-graphql's `Maybe`.

Used to distinguish between leaving a value untouched and writing a value,
where the value written may itself be `None`:

* `None`: Leave the value untouched.
* `Some(None)`: Write `None`, i.e. clear the value.
* `Some(value)`: Write `value`.
"""

from dataclasses import dataclass
from typing import Generic
from typing import TypeAlias
from typing import TypeVar

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Some(Generic[T]):
    value: T


Maybe: TypeAlias = Some[T] | None
