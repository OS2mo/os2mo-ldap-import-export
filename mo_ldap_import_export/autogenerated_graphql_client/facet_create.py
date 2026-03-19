from uuid import UUID

from .base_model import BaseModel


class FacetCreate(BaseModel):
    facet_create: "FacetCreateFacetCreate"


class FacetCreateFacetCreate(BaseModel):
    uuid: UUID


FacetCreate.update_forward_refs()
FacetCreateFacetCreate.update_forward_refs()
