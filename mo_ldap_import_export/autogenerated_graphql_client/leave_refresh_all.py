from uuid import UUID

from .base_model import BaseModel


class LeaveRefreshAll(BaseModel):
    leave_refresh: "LeaveRefreshAllLeaveRefresh"


class LeaveRefreshAllLeaveRefresh(BaseModel):
    objects: list[UUID]


LeaveRefreshAll.update_forward_refs()
LeaveRefreshAllLeaveRefresh.update_forward_refs()
