from datetime import datetime
from typing import Any
from uuid import UUID

from .base_model import BaseModel


class ReadItusersWithValidity(BaseModel):
    itusers: "ReadItusersWithValidityItusers"


class ReadItusersWithValidityItusers(BaseModel):
    objects: list["ReadItusersWithValidityItusersObjects"]
    page_info: "ReadItusersWithValidityItusersPageInfo"


class ReadItusersWithValidityItusersObjects(BaseModel):
    uuid: UUID
    validities: list["ReadItusersWithValidityItusersObjectsValidities"]


class ReadItusersWithValidityItusersObjectsValidities(BaseModel):
    validity: "ReadItusersWithValidityItusersObjectsValiditiesValidity"


class ReadItusersWithValidityItusersObjectsValiditiesValidity(BaseModel):
    to: datetime | None


class ReadItusersWithValidityItusersPageInfo(BaseModel):
    next_cursor: Any | None


ReadItusersWithValidity.update_forward_refs()
ReadItusersWithValidityItusers.update_forward_refs()
ReadItusersWithValidityItusersObjects.update_forward_refs()
ReadItusersWithValidityItusersObjectsValidities.update_forward_refs()
ReadItusersWithValidityItusersObjectsValiditiesValidity.update_forward_refs()
ReadItusersWithValidityItusersPageInfo.update_forward_refs()
