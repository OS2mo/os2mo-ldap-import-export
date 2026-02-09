#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
# --------------------------------------------------------------------------------------
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Protocol
from typing import Union
from uuid import UUID

from fastapi.encoders import jsonable_encoder
from pydantic import AnyHttpUrl

from ..auth import AuthenticatedAsyncHTTPXClient
from ..auth import keycloak_token_endpoint
from ..modelclient.base import ModelClientBase


class MOBase(Protocol):
    def dict(self) -> dict:  # pragma: no cover
        ...

    @property
    def uuid(self) -> UUID:  # pragma: no cover
        ...


class ModelClient(ModelClientBase[MOBase]):
    upload_http_method = "POST"
    create_path_map: Dict[str, str] = {
        "Address": "/service/details/create",
        "Association": "/service/details/create",
        "Employee": "/service/e/create",
        "Engagement": "/service/details/create",
        "EngagementAssociation": "/service/details/create",
        "ClassWrite": "/service/f/{facet_uuid}/",
        "ITUser": "/service/details/create",
        "KLE": "/service/details/create",
        "Leave": "/service/details/create",
        "Manager": "/service/details/create",
        "OrganisationUnit": "/service/ou/create",
        "Role": "/service/details/create",
    }
    edit_path_map: Dict[str, str] = {
        "Address": "/service/details/edit",
        "Association": "/service/details/edit",
        "Employee": "/service/details/edit",
        "Engagement": "/service/details/edit",
        "ClassWrite": "/service/f/{facet_uuid}/",
        "ITUser": "/service/details/edit",
        "KLE": "/service/details/edit",
        "Leave": "/service/details/edit",
        "Manager": "/service/details/edit",
        "OrganisationUnit": "/service/details/edit",
        "Role": "/service/details/edit",
    }
    async_httpx_client_class = AuthenticatedAsyncHTTPXClient

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        auth_realm: str,
        auth_server: AnyHttpUrl,
        force: bool = False,
        *args: Any,
        **kwargs: Any,
    ):
        """MO ModelClient.

        Args:
            client_id: Keycloak client id used for authentication.
            client_secret: Keycloak client secret used for authentication.
            auth_realm: Keycloak auth realm used for authentication.
            auth_server: URL of the Keycloak server used for authentication.
            force: Bypass MO API validation.
            *args: Positional arguments passed through to ModelClientBase.
            **kwargs: Keyword arguments passed through to ModelClientBase.

        Example usage::

            async with ModelClient(
                base_url="http://mo:5000",
                client_id="AzureDiamond",
                client_secret="hunter2",
                auth_server=parse_obj_as(AnyHttpUrl,"https://keycloak.example.org/auth"),
                auth_realm="mordor",
            ) as client:
                r = await client.upload(objects)
        """
        super().__init__(
            *args,
            client_id=client_id,
            client_secret=client_secret,
            token_endpoint=keycloak_token_endpoint(
                auth_server=auth_server,
                auth_realm=auth_realm,
            ),
            **kwargs,
        )
        self.force = force

    def get_object_url(
        self, obj: MOBase, *args: Any, edit: bool = False, **kwargs: Any
    ) -> str:
        # Note that we additionally format the object's fields onto the path mapping to
        # support schemes such as /service/f/{facet_uuid}/, where facet_uuid is
        # retrieved from obj.facet_uuid.
        path_map = self.edit_path_map if edit else self.create_path_map
        path = path_map[type(obj).__name__].format_map(obj.dict())
        return f"{path}?force={int(self.force)}"

    def get_object_json(
        self, obj: Union[MOBase, Any], *args: Any, edit: bool = False, **kwargs: Any
    ) -> Any:
        # 'jsonable_encoder' is used directly on the obj, as 'exclude_defaults' doesn't
        # work when the object is nested within a dict.
        # TODO: Ideally we'd want to use 'exclude_unset' instead of 'exclude_defaults',
        #  but it doesn't seem to work properly with our models.
        if edit:
            obj = {
                "uuid": obj.uuid,
                "type": obj.type_,  # type: ignore[union-attr]
                "data": jsonable_encoder(obj, exclude_defaults=True),
            }
        return super().get_object_json(obj, *args, **kwargs)

    async def edit(
        self, objs: Iterable[MOBase], *args: Any, **kwargs: Any
    ) -> List[Any]:
        return await self.upload(objs, *args, edit=True, **kwargs)
