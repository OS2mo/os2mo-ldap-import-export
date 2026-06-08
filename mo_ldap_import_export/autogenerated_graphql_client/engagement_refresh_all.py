from uuid import UUID

from .base_model import BaseModel


class EngagementRefreshAll(BaseModel):
    engagement_refresh: "EngagementRefreshAllEngagementRefresh"


class EngagementRefreshAllEngagementRefresh(BaseModel):
    objects: list[UUID]


EngagementRefreshAll.update_forward_refs()
EngagementRefreshAllEngagementRefresh.update_forward_refs()
