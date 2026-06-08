from uuid import UUID

from .base_model import BaseModel


class ItuserRefreshAll(BaseModel):
    ituser_refresh: "ItuserRefreshAllItuserRefresh"


class ItuserRefreshAllItuserRefresh(BaseModel):
    objects: list[UUID]


ItuserRefreshAll.update_forward_refs()
ItuserRefreshAllItuserRefresh.update_forward_refs()
