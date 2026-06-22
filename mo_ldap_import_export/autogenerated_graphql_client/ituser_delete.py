from uuid import UUID

from .base_model import BaseModel


class ItuserDelete(BaseModel):
    ituser_delete: "ItuserDeleteItuserDelete"


class ItuserDeleteItuserDelete(BaseModel):
    uuid: UUID


ItuserDelete.update_forward_refs()
ItuserDeleteItuserDelete.update_forward_refs()
