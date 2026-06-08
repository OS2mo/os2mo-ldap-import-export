from uuid import UUID

from .base_model import BaseModel


class KleRefreshAll(BaseModel):
    kle_refresh: "KleRefreshAllKleRefresh"


class KleRefreshAllKleRefresh(BaseModel):
    objects: list[UUID]


KleRefreshAll.update_forward_refs()
KleRefreshAllKleRefresh.update_forward_refs()
