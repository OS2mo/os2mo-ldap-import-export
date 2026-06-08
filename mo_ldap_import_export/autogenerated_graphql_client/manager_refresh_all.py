from uuid import UUID

from .base_model import BaseModel


class ManagerRefreshAll(BaseModel):
    manager_refresh: "ManagerRefreshAllManagerRefresh"


class ManagerRefreshAllManagerRefresh(BaseModel):
    objects: list[UUID]


ManagerRefreshAll.update_forward_refs()
ManagerRefreshAllManagerRefresh.update_forward_refs()
