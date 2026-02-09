#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
# --------------------------------------------------------------------------------------
import asyncio
import itertools
from functools import partial
from typing import Any
from typing import AsyncIterator
from typing import Dict
from typing import Generic
from typing import Iterable
from typing import List
from typing import Type
from typing import TypeVar
from typing import Union

import more_itertools
from fastapi.encoders import jsonable_encoder
from httpx import AsyncClient
from httpx import HTTPStatusError
from structlog import get_logger
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential
from tqdm import tqdm

logger = get_logger()


class ModelClientException(Exception):
    pass


ModelBase = TypeVar("ModelBase")


class ModelClientBase(Generic[ModelBase]):
    upload_http_method: str
    path_map: Dict[str, str]
    async_httpx_client_class: Type[AsyncClient]

    def __init__(self, *args: Any, chunk_size: int = 10, **kwargs: Any) -> None:
        """Base ModelClient.

        Args:
            *args: Positional arguments passed through to AuthenticatedAsyncHTTPXClient.
            chunk_size: Size of the chunks objects are split into before being uploaded
             in parallel.
            **kwargs: Keyword arguments passed through to AuthenticatedAsyncHTTPXClient.
        """
        super().__init__()
        self.async_httpx_client = self.async_httpx_client_class(*args, **kwargs)
        self.chunk_size = chunk_size

    def get_object_url(
        self, obj: ModelBase, *args: Any, **kwargs: Any
    ) -> str:  # pragma: no cover
        return self.path_map[type(obj).__name__]

    def get_object_json(
        self, obj: Union[ModelBase, Any], *args: Any, **kwargs: Any
    ) -> Any:
        return jsonable_encoder(obj)

    @retry(
        retry=retry_if_exception_type(HTTPStatusError),
        reraise=True,
        wait=wait_random_exponential(multiplier=2, max=30),
        stop=stop_after_attempt(3),
        after=lambda rs: logger.warning(f"Upload failed ({rs.attempt_number}/3)."),
    )
    async def upload_object(self, obj: ModelBase, *args: Any, **kwargs: Any) -> Any:
        response = await self.async_httpx_client.request(
            self.upload_http_method,
            self.get_object_url(obj, *args, **kwargs),
            json=self.get_object_json(obj, *args, **kwargs),
        )
        response_json = response.json()
        try:
            response.raise_for_status()
        except HTTPStatusError as error:
            if "description" in response_json:
                raise HTTPStatusError(
                    message=response_json["description"],
                    request=error.request,
                    response=error.response,
                ) from error
            raise error
        return response_json

    async def upload_lazy(
        self, objs: Iterable[ModelBase], *args: Any, **kwargs: Any
    ) -> AsyncIterator[Any]:
        objs = list(objs)  # len() is unfortunately needed for proper progress bar
        with tqdm(total=len(objs), unit="object") as progress_bar:
            for object_type, group in itertools.groupby(objs, key=type):
                progress_bar.set_description(object_type.__name__)
                for chunk in more_itertools.chunked(group, n=self.chunk_size):
                    tasks = map(partial(self.upload_object, *args, **kwargs), chunk)
                    for task in asyncio.as_completed(tasks):
                        yield await task
                        progress_bar.update()

    async def upload(
        self, objs: Iterable[ModelBase], *args: Any, **kwargs: Any
    ) -> List[Any]:
        return [x async for x in self.upload_lazy(objs, *args, **kwargs)]

    async def __aenter__(self):  # type: ignore
        # TODO: fix return type to Self when we get to a Python version >=3.11
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore
        await self.async_httpx_client.aclose()
