# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import hashlib
from functools import lru_cache
from functools import partial
from typing import Callable
from uuid import UUID


@lru_cache(maxsize=None)
def _generate_uuid(value: str) -> UUID:
    value_hash = hashlib.md5(value.encode())
    value_digest = value_hash.hexdigest()
    return UUID(value_digest)


def generate_uuid(base: str, value: str) -> UUID:
    """Generate a predictable uuid based on two variables.

    Example:
        ```Python
        uuid1 = uuid_generator("secret_seed", "facetnavn1")
        uuid2 = uuid_generator("secret_seed", "facetnavn1")
        uuid3 = uuid_generator("secret_seed", "facetnavn2")
        uuid4 = uuid_generator("s3cr3t_s33d", "facetnavn1")
        assert uuid1 == uuid2
        assert uuid1 != uuid3
        assert uuid1 != uuid4
        ```

    Args:
        base: Base or seed utilized to generate UUID.
        value: Specific evalue utilized to generate UUID.

    Returns:
        The generated UUID
    """
    base_uuid = _generate_uuid(base)
    return _generate_uuid(str(base_uuid) + str(value))


def uuid_generator(base: str) -> Callable[[str], UUID]:
    """Construct an UUID generator with a fixed base/seed.

    Example:
        ```Python
        uuid_gen = uuid_generator("secret_seed")
        uuid1 = uuid_gen("facetnavn1")
        uuid2 = uuid_gen("facetnavn1")
        uuid3 = uuid_gen("facetnavn2")
        assert uuid1 == uuid2
        assert uuid1 != uuid3
        ```

    Args:
        base: Base or seed utilized to generate all UUIDs.

    Returns:
        Function which map strings to UUIDs.
    """
    return partial(generate_uuid, base)
