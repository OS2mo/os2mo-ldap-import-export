# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import warnings
from asyncio import gather
from asyncio.exceptions import TimeoutError
from collections import ChainMap
from enum import Enum
from functools import partial
from itertools import starmap
from operator import itemgetter
from types import TracebackType
from typing import Any
from typing import ChainMap as tChainMap
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Type
from typing import cast
from uuid import UUID

import aiohttp
from more_itertools import chunked
from more_itertools import one
from more_itertools import unzip
from tenacity import retry
from tenacity import stop_after_delay
from tenacity import wait_exponential

from fastramqpi.ra_utils.syncable import Syncable

retry_max_time = 30

# TODO: Pydantic type: #45518
AddressReply = Dict[str, Any]


class AddressType(str, Enum):
    """Enumeration of possible address-types / endpoints."""

    ADDRESS = "adresser"
    """Addresses are the main entry / lookup in DAR.

    A single address may have multiple `ACCESS_ADDRESS`es.
    """

    ACCESS_ADDRESS = "adgangsadresser"
    """Access addresses are sub entries in DAR.

    Multiple access addresses may share a single `Address`, for instance:

    Example:
        `ADDRESS`: `ApartmentComplex 1`, and `ACCESS_ADDRESS`es:

        * `ApartmentComplex 1, tv`
        * `ApartmentComplex 1, th`
        * `ApartmentComplex 1, lf`
    """

    HISTORIC_ADDRESS = "historik/adresser"
    """Historic version of `ADDRESS`"""

    HISTORIC_ACCESS_ADDRESS = "historik/adgangsadresser"
    """Historic version of `ACCESS_ADDRESS`"""


ALL_ADDRESS_TYPES = list(AddressType)


class AsyncDARClient:
    """Asynchronous DAR client.

    Example:
        ```Python
        from os2mo_dar_client import AsyncDARClient

        adarclient = AsyncDARClient()
        async with adarclient:
            print(await adarclient.healthcheck())
        ```
    """

    # TODO: Query endpoints ala dawa_helper.py: #45522
    # TODO: Autocomplete endpoints ala OS2mo: #45521
    # TODO: Caching: #45519

    def __init__(self, timeout: int = 10) -> None:
        """Construct an async DAR client.

        Args:
            timeout: Maximum waiting time for response.
        """
        self._timeout: int = timeout

        self._session: Optional[aiohttp.ClientSession] = None
        self._baseurl: str = "https://api.dataforsyningen.dk"

    async def __aenter__(self) -> "AsyncDARClient":
        await self.aopen()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        exc_traceback: Optional[TracebackType],
    ) -> bool:
        await self.aclose()
        return False

    async def aopen(self) -> None:
        if self._session:
            warnings.warn("aopen called with existing session", UserWarning)
            return
        connector = aiohttp.TCPConnector(limit=10)
        self._session = aiohttp.ClientSession(connector=connector)

    async def aclose(self) -> None:
        if self._session is None:
            warnings.warn("aclose called without session", UserWarning)
            return
        await self._session.close()
        self._session = None

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise ValueError("Session not set")
        return self._session

    async def healthcheck(self, timeout: Optional[int] = None) -> bool:
        """Check whether DAR can be reached

        Args:
            timeout: Maximum waiting time for response, defaults to class timeout.

        Returns:
            `True` if reachable, `False` otherwise.
        """
        url = f"{self._baseurl}/autocomplete"
        try:
            async with self._get_session().get(
                url, timeout=aiohttp.ClientTimeout(timeout or self._timeout)
            ) as response:
                if response.status == 200:
                    return True
                return False
        except aiohttp.ClientError:
            return False
        except TimeoutError:
            return False

    async def _cleanse_single(
        self, address_string: str, addrtype: AddressType
    ) -> AddressReply:
        """Request that DAR cleanse a string address for us.

        The cleansing process attempts to convert an unstructured text string into a
        stuctured address reply. The process may fail if the input string is too vague.

        Args:
            address_string: The address string we wish to cleanse.
            addrtype: The address type to lookup.

        Raises:
            aiohttp.ClientResponseError: If anything goes wrong.
            ValueError: If passed a historic addrtype.
            RuntimeError: If one unique match could not be found.

        Returns:
            * dict: DAR Reply
        """
        if addrtype in [
            AddressType.HISTORIC_ADDRESS,
            AddressType.HISTORIC_ACCESS_ADDRESS,
        ]:
            raise ValueError("DAR does not support historic cleansing")

        url = f"{self._baseurl}/datavask/{addrtype.value}"
        params: dict[str, str] = {"betegnelse": address_string}

        async with self._get_session().get(
            url, params=params, timeout=aiohttp.ClientTimeout(self._timeout)
        ) as response:
            response.raise_for_status()
            payload = await response.json()
            # Check match category:
            # A is a near perfect match,
            # B is a unique match,
            # C is a non-unique match (which we do not accept)
            if payload["kategori"] not in ["A", "B"]:
                raise RuntimeError("DAR was unable to find a conclusive match")
            address = one(payload["resultater"])["adresse"]
            return cast(AddressReply, address)

    # TODO: Caching goes in here
    async def _address_fetched(self, uuid: UUID, reply: Dict[str, Any]) -> None:
        pass

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_delay(retry_max_time),
    )
    async def _fetch_single(self, uuid: UUID, addrtype: AddressType) -> AddressReply:
        """Lookup uuid in DAR.

        Args:
            uuid: DAR UUID.
            addrtypes: The address type to lookup.

        Raises:
            aiohttp.ClientResponseError: If anything goes wrong

        Returns:
            * dict: DAR Reply
        """

        url = f"{self._baseurl}/{addrtype.value}/{str(uuid)}"
        params: dict[str, str | int] = {"struktur": "mini", "noformat": 1}

        async with self._get_session().get(
            url, params=params, timeout=aiohttp.ClientTimeout(self._timeout)
        ) as response:
            response.raise_for_status()
            payload = await response.json()
            return cast(AddressReply, payload)

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_delay(retry_max_time),
    )
    async def _fetch_non_chunked(
        self, uuids: Set[UUID], addrtype: AddressType
    ) -> Tuple[Dict[UUID, AddressReply], Set[UUID]]:
        """Lookup uuids in DAR (no chunking).

        Args:
            uuids: List of DAR UUIDs to lookup.
            addrtype: The address type to lookup.

        Returns:
            * dict: Map from UUID to DAR reply.
            * set: Set of UUIDs of entries which were not found.
        """
        url = f"{self._baseurl}/{addrtype.value}"
        params: dict[str, str | int] = {
            "id": "|".join(map(str, uuids)),
            "struktur": "mini",
            "noformat": 1,
        }

        async with self._get_session().get(
            url, params=params, timeout=aiohttp.ClientTimeout(self._timeout)
        ) as response:
            response.raise_for_status()
            body = await response.json()

            result_uuids = map(UUID, map(itemgetter("id"), body))
            result = dict(zip(result_uuids, body))

            found_uuids = result.keys()
            missing = set(uuids) - found_uuids

            return result, missing

    async def _fetch_chunked(
        self, uuids: Set[UUID], addrtype: AddressType, chunk_size: int
    ) -> Tuple[Dict[UUID, AddressReply], Set[UUID]]:
        """Lookup uuids in DAR (chunked).

        Chunks the UUIDs before calling `_fetch_non_chunked` on each chunk.

        Args:
            uuids: List of DAR UUIDs.
            addrtype: The address type to lookup.
            chunk_size: Number of UUIDs per block, sent to DAR.

        Returns:
            * dict: Map from UUID to DAR reply.
            * set: Set of UUIDs of entries which were not found.
        """

        # Chunk our UUIDs into blocks of chunk_size
        uuid_chunks = chunked(uuids, chunk_size)
        # Convert chunks into a list of asyncio.tasks
        tasks = map(partial(self._fetch_non_chunked, addrtype=addrtype), uuid_chunks)  # type: ignore
        # Here 'result' is a list of tuples (dict, set) => (result, missing)
        result = await gather(*tasks)
        # First we unzip 'result' to get a list of results and a list of missing
        result_dicts, missing_sets = unzip(result)
        # Then we union the dicts and sets before returning
        combined_result = dict(ChainMap(*result_dicts))  # type: ignore
        combined_missing = set.union(*missing_sets)  # type: ignore
        return combined_result, combined_missing  # type: ignore

    async def _fetch(
        self, uuids: Set[UUID], addrtype: AddressType, chunk_size: int
    ) -> Tuple[Dict[UUID, AddressReply], Set[UUID]]:
        """Lookup uuids in DAR (chunked if required).

        Args:
            uuids: List of DAR UUIDs.
            addrtype: The address type to lookup.
            chunk_size: Number of UUIDs per block, sent to DAR.

        Returns:
            * dict: Map from UUID to DAR reply.
            * set: Set of UUIDs of entries which were not found.
        """
        num_uuids = len(uuids)
        # Short-circuit if possible, chunk if required
        if num_uuids == 0:
            return dict(), set()
        if num_uuids <= chunk_size:
            res: Tuple[
                Dict[UUID, AddressReply], Set[UUID]
            ] = await self._fetch_non_chunked(uuids, addrtype)
            return res
        return await self._fetch_chunked(uuids, addrtype, chunk_size)

    async def fetch(
        self,
        uuids: Set[UUID],
        addrtypes: Optional[List[AddressType]] = None,
        # WARNING: DAR does not support paths of more than 4096 characters on
        # HTTP/1.1. aiohttp does not support HTTP/2. Do not increase the
        # `chunk_size` without testing irl.
        chunk_size: int = 100,
    ) -> Tuple[Dict[UUID, AddressReply], Set[UUID]]:
        """Lookup uuids in DAR (chunked if necessary).

        Calls `_fetch` with all the provided addrtypes.

        Args:
            uuids: List of DAR UUIDs.
            addrtypes: The address type(s) to lookup. If `None` all 4 types are checked.
            chunk_size: Number of UUIDs per block, sent to DAR.

        Returns:
            * dict: Map from UUID to DAR reply.
            * set: Set of UUIDs of entries which were not found.
        """
        addrtypes = addrtypes or ALL_ADDRESS_TYPES
        combined_result: tChainMap[UUID, AddressReply] = ChainMap({})
        # TODO: Do all 4 in parallel?
        for addrtype in addrtypes:
            result, missing = await self._fetch(uuids, addrtype, chunk_size=chunk_size)
            combined_result = ChainMap(combined_result, result)
            # If we managed to find everything, there is no need to check the remaining
            # address types, we can simply return our findings
            if not missing:
                break
            # We only need to check the missing UUIDs in the remaining address types
            uuids = missing
        final_result = dict(combined_result)
        await gather(*starmap(self._address_fetched, final_result.items()))
        return final_result, missing

    async def fetch_single(
        self, uuid: UUID, addrtypes: Optional[List[AddressType]] = None
    ) -> AddressReply:
        """Lookup uuid in DAR.

        Calls `_fetch_single` with all the provided addrtypes.

        Args:
            uuid: DAR UUID.
            addrtypes: The address type(s) to lookup. If `None` all 4 types are checked.

        Raises:
            ValueError: If no match could be found

        Returns:
            * dict: DAR Reply
        """
        addrtypes = addrtypes or ALL_ADDRESS_TYPES
        # TODO: Do all 4 in parallel?
        for addrtype in addrtypes:
            try:
                payload: AddressReply = await self._fetch_single(uuid, addrtype)
                # If we get here everything went well
                await self._address_fetched(uuid, payload)
                return payload
            except aiohttp.ClientResponseError as exc:
                # If not found, try the next address type
                if exc.status == 404:
                    continue
                raise exc
        raise ValueError("No address match found in DAR")

    async def cleanse_single(
        self, address_string: str, addrtypes: Optional[List[AddressType]] = None
    ) -> AddressReply:
        """Request that DAR cleanse a string address for us.

        The cleansing process attempts to convert an unstructured text string into a
        stuctured address reply. The process may fail if the input string is too vague.

        Calls `_cleanse_single` with appropriate addrtypes.

        Args:
            address_string: The address string we wish to cleanse.
            addrtypes: The address type(s) to lookup. If `None` all 4 types are checked.

        Raises:
            aiohttp.ClientResponseError: If anything goes wrong.
            ValueError: If passed a historic addrtype.
            RuntimeError: If one unique match could not be found.

        Returns:
            * dict: DAR Reply
        """
        addrtypes = addrtypes or [AddressType.ADDRESS, AddressType.ACCESS_ADDRESS]
        # TODO: Do all in parallel?
        for addrtype in addrtypes:
            try:
                payload = await self._cleanse_single(address_string, addrtype)
                return payload
            except aiohttp.ClientResponseError as exc:
                # If not found, try the next address type
                if exc.status == 404:
                    continue
                raise exc
            except RuntimeError:
                # If we did not find a conclusive match, try the next address type
                continue
        raise ValueError("No address match found from cleansing in DAR")


class DARClient(Syncable, AsyncDARClient):
    """Synchronous DAR client.

        Example:
            ```Python
            from os2mo_dar_client import DARClient

            darclient = DARClient()
            with darclient:
                print(darclient.healthcheck())
            ```

    Is implemented atop the `AsyncDARClient` using `ra_utils.Syncable`.
    """

    pass
