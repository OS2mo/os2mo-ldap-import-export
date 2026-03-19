from uuid import UUID

from .base_model import BaseModel


class OrgCreate(BaseModel):
    org_create: "OrgCreateOrgCreate"


class OrgCreateOrgCreate(BaseModel):
    uuid: UUID


OrgCreate.update_forward_refs()
OrgCreateOrgCreate.update_forward_refs()
