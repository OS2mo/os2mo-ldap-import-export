from uuid import UUID

from .base_model import BaseModel


class ItsystemDelete(BaseModel):
    itsystem_delete: "ItsystemDeleteItsystemDelete"


class ItsystemDeleteItsystemDelete(BaseModel):
    uuid: UUID


ItsystemDelete.update_forward_refs()
ItsystemDeleteItsystemDelete.update_forward_refs()
