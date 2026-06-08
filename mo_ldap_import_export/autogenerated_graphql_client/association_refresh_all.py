from uuid import UUID

from .base_model import BaseModel


class AssociationRefreshAll(BaseModel):
    association_refresh: "AssociationRefreshAllAssociationRefresh"


class AssociationRefreshAllAssociationRefresh(BaseModel):
    objects: list[UUID]


AssociationRefreshAll.update_forward_refs()
AssociationRefreshAllAssociationRefresh.update_forward_refs()
