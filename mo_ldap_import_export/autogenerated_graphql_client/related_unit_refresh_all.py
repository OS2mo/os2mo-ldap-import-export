from uuid import UUID

from .base_model import BaseModel


class RelatedUnitRefreshAll(BaseModel):
    related_unit_refresh: "RelatedUnitRefreshAllRelatedUnitRefresh"


class RelatedUnitRefreshAllRelatedUnitRefresh(BaseModel):
    objects: list[UUID]


RelatedUnitRefreshAll.update_forward_refs()
RelatedUnitRefreshAllRelatedUnitRefresh.update_forward_refs()
