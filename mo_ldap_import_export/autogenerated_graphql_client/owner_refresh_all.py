from uuid import UUID

from .base_model import BaseModel


class OwnerRefreshAll(BaseModel):
    owner_refresh: "OwnerRefreshAllOwnerRefresh"


class OwnerRefreshAllOwnerRefresh(BaseModel):
    objects: list[UUID]


OwnerRefreshAll.update_forward_refs()
OwnerRefreshAllOwnerRefresh.update_forward_refs()
