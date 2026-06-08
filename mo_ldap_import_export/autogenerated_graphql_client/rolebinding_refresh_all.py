from uuid import UUID

from .base_model import BaseModel


class RolebindingRefreshAll(BaseModel):
    rolebinding_refresh: "RolebindingRefreshAllRolebindingRefresh"


class RolebindingRefreshAllRolebindingRefresh(BaseModel):
    objects: list[UUID]


RolebindingRefreshAll.update_forward_refs()
RolebindingRefreshAllRolebindingRefresh.update_forward_refs()
