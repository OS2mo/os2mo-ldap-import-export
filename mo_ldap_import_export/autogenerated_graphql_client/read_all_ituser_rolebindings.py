from typing import Any
from uuid import UUID

from .base_model import BaseModel


class ReadAllItuserRolebindings(BaseModel):
    rolebindings: "ReadAllItuserRolebindingsRolebindings"


class ReadAllItuserRolebindingsRolebindings(BaseModel):
    objects: list["ReadAllItuserRolebindingsRolebindingsObjects"]
    page_info: "ReadAllItuserRolebindingsRolebindingsPageInfo"


class ReadAllItuserRolebindingsRolebindingsObjects(BaseModel):
    uuid: UUID


class ReadAllItuserRolebindingsRolebindingsPageInfo(BaseModel):
    next_cursor: Any | None


ReadAllItuserRolebindings.update_forward_refs()
ReadAllItuserRolebindingsRolebindings.update_forward_refs()
ReadAllItuserRolebindingsRolebindingsObjects.update_forward_refs()
ReadAllItuserRolebindingsRolebindingsPageInfo.update_forward_refs()
