from uuid import UUID

from .base_model import BaseModel


class OrgUnitRefreshAll(BaseModel):
    org_unit_refresh: "OrgUnitRefreshAllOrgUnitRefresh"


class OrgUnitRefreshAllOrgUnitRefresh(BaseModel):
    objects: list[UUID]


OrgUnitRefreshAll.update_forward_refs()
OrgUnitRefreshAllOrgUnitRefresh.update_forward_refs()
