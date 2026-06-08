from uuid import UUID

from .base_model import BaseModel


class ItsystemRefreshAll(BaseModel):
    itsystem_refresh: "ItsystemRefreshAllItsystemRefresh"


class ItsystemRefreshAllItsystemRefresh(BaseModel):
    objects: list[UUID]


ItsystemRefreshAll.update_forward_refs()
ItsystemRefreshAllItsystemRefresh.update_forward_refs()
