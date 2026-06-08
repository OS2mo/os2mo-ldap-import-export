from uuid import UUID

from .base_model import BaseModel


class ClassRefreshAll(BaseModel):
    class_refresh: "ClassRefreshAllClassRefresh"


class ClassRefreshAllClassRefresh(BaseModel):
    objects: list[UUID]


ClassRefreshAll.update_forward_refs()
ClassRefreshAllClassRefresh.update_forward_refs()
