from typing import Any
from typing import Optional
from uuid import UUID

from .base_model import BaseModel


class ReadOrphanedAddresses(BaseModel):
    addresses: "ReadOrphanedAddressesAddresses"


class ReadOrphanedAddressesAddresses(BaseModel):
    objects: list["ReadOrphanedAddressesAddressesObjects"]
    page_info: "ReadOrphanedAddressesAddressesPageInfo"


class ReadOrphanedAddressesAddressesObjects(BaseModel):
    current: Optional["ReadOrphanedAddressesAddressesObjectsCurrent"]


class ReadOrphanedAddressesAddressesObjectsCurrent(BaseModel):
    uuid: UUID
    user_key: str
    value: str
    ituser_response: Optional[
        "ReadOrphanedAddressesAddressesObjectsCurrentItuserResponse"
    ]


class ReadOrphanedAddressesAddressesObjectsCurrentItuserResponse(BaseModel):
    current: Optional[
        "ReadOrphanedAddressesAddressesObjectsCurrentItuserResponseCurrent"
    ]


class ReadOrphanedAddressesAddressesObjectsCurrentItuserResponseCurrent(BaseModel):
    uuid: UUID
    user_key: str
    external_id: str | None


class ReadOrphanedAddressesAddressesPageInfo(BaseModel):
    next_cursor: Any | None


ReadOrphanedAddresses.update_forward_refs()
ReadOrphanedAddressesAddresses.update_forward_refs()
ReadOrphanedAddressesAddressesObjects.update_forward_refs()
ReadOrphanedAddressesAddressesObjectsCurrent.update_forward_refs()
ReadOrphanedAddressesAddressesObjectsCurrentItuserResponse.update_forward_refs()
ReadOrphanedAddressesAddressesObjectsCurrentItuserResponseCurrent.update_forward_refs()
ReadOrphanedAddressesAddressesPageInfo.update_forward_refs()
