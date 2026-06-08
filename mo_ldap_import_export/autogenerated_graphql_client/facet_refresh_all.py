from uuid import UUID

from .base_model import BaseModel


class FacetRefreshAll(BaseModel):
    facet_refresh: "FacetRefreshAllFacetRefresh"


class FacetRefreshAllFacetRefresh(BaseModel):
    objects: list[UUID]


FacetRefreshAll.update_forward_refs()
FacetRefreshAllFacetRefresh.update_forward_refs()
