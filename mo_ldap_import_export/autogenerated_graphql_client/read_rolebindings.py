from typing import Optional
from uuid import UUID

from .base_model import BaseModel


class ReadRolebindings(BaseModel):
    rolebindings: "ReadRolebindingsRolebindings"


class ReadRolebindingsRolebindings(BaseModel):
    objects: list["ReadRolebindingsRolebindingsObjects"]


class ReadRolebindingsRolebindingsObjects(BaseModel):
    uuid: UUID
    current: Optional["ReadRolebindingsRolebindingsObjectsCurrent"]


class ReadRolebindingsRolebindingsObjectsCurrent(BaseModel):
    user_key: str
    role: list["ReadRolebindingsRolebindingsObjectsCurrentRole"]
    ituser: list["ReadRolebindingsRolebindingsObjectsCurrentItuser"]


class ReadRolebindingsRolebindingsObjectsCurrentRole(BaseModel):
    uuid: UUID


class ReadRolebindingsRolebindingsObjectsCurrentItuser(BaseModel):
    uuid: UUID


ReadRolebindings.update_forward_refs()
ReadRolebindingsRolebindings.update_forward_refs()
ReadRolebindingsRolebindingsObjects.update_forward_refs()
ReadRolebindingsRolebindingsObjectsCurrent.update_forward_refs()
ReadRolebindingsRolebindingsObjectsCurrentRole.update_forward_refs()
ReadRolebindingsRolebindingsObjectsCurrentItuser.update_forward_refs()
