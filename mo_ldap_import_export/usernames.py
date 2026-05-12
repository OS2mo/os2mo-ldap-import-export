# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from collections.abc import Iterator

import structlog
from ldap3 import NO_ATTRIBUTES
from ldap3 import Connection
from ldap3.utils.conv import escape_filter_chars

from .config import Settings
from .ldap import object_search
from .models import Employee
from .utils import combine_dn_strings

logger = structlog.stdlib.get_logger()


def generate_person_name(employee: Employee) -> list[str]:
    assert employee.given_name is not None
    assert employee.surname is not None
    given_name = employee.given_name
    surname = employee.surname
    name = given_name.split(" ")[:4] + [surname]
    return name


class UserNameGenerator:
    """
    Class with functions to generate valid LDAP usernames.

    Each customer could have his own username generator function in here. All you need
    to do, is refer to the proper function inside the json dict.
    """

    def __init__(self, settings: Settings, ldap_connection: Connection) -> None:
        self.settings = settings
        self.ldap_connection = ldap_connection

    def _make_cn(self, username_string: str):
        return f"CN={username_string}"

    def _make_dn(self, username_string: str) -> str:
        cn = self._make_cn(username_string)
        dn = combine_dn_strings(
            [
                cn,
                self.settings.ldap_ou_for_new_users,
                self.settings.ldap_search_base,
            ]
        )

        return dn

    async def _create_common_name(
        self, name: list[str], current_common_name: str | None = None
    ) -> str:
        """
        Create an LDAP-style common name (CN) based on first and last name

        If a name exists, "_2" is added. If that one also exists, "_3" is added,
        and so on

        Examples
        -------------
        >>> _create_common_name(["Keanu","Reeves"])
        >>> "Keanu Reeves"
        """

        def permutations(username: str) -> Iterator[str]:
            yield username
            for permutation_counter in range(2, 1000):
                yield username + "_" + str(permutation_counter)

        name = [n for n in name if n]
        num_middlenames = len(name) - 2

        # Shorten a name if it is over 64 chars
        # see http://msdn.microsoft.com/en-us/library/ms675449(VS.85).aspx
        common_name = " ".join(name)
        while len(common_name) > 60 and num_middlenames > 0:
            # Remove one middlename
            num_middlenames -= 1
            # Try to make a name with the selected number of middlenames
            given_name, *middlenames, surname = name
            middlenames = middlenames[:num_middlenames]
            common_name = " ".join([given_name] + middlenames + [surname])

        # Cut off the name (leave place for the permutation counter)
        common_name = common_name[:60]

        for potential_name in permutations(common_name):
            # We allow the candidate to match the current common name, as the entity
            # is otherwise considered to conflict with itself.
            if (
                current_common_name
                and potential_name.lower() == current_common_name.lower()
            ):
                return potential_name
            if await self._common_name_exists(potential_name):
                continue
            return potential_name

        # TODO: Return a more specific exception type
        raise RuntimeError("Failed to create common name")

    async def _common_name_exists(self, candidate: str) -> bool:
        searchParameters = {
            "search_base": self.settings.ldap_search_base,
            "search_filter": f"(cn={escape_filter_chars(candidate)})",
            "attributes": NO_ATTRIBUTES,
            "size_limit": 1,
        }
        search_result = await object_search(searchParameters, self.ldap_connection)
        return bool(search_result)

    async def generate_common_name(
        self, employee: Employee, current_common_name: str | None = None
    ) -> str:
        name = generate_person_name(employee)
        common_name = await self._create_common_name(name, current_common_name)
        logger.info(
            "Generated CommonName based on name",
            name=name,
            common_name=common_name,
        )
        return common_name

    async def generate_dn(self, common_name: str) -> str:
        """
        Generates a LDAP DN (Distinguished Name) based on information from a MO common name.
        """
        dn = self._make_dn(common_name)
        return dn
