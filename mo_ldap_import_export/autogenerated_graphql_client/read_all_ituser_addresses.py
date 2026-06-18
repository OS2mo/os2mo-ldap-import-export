from typing import Any
from uuid import UUID

from .base_model import BaseModel


class ReadAllItuserAddresses(BaseModel):
    addresses: "ReadAllItuserAddressesAddresses"


class ReadAllItuserAddressesAddresses(BaseModel):
    objects: list["ReadAllItuserAddressesAddressesObjects"]
    page_info: "ReadAllItuserAddressesAddressesPageInfo"


class ReadAllItuserAddressesAddressesObjects(BaseModel):
    uuid: UUID


class ReadAllItuserAddressesAddressesPageInfo(BaseModel):
    next_cursor: Any | None


ReadAllItuserAddresses.update_forward_refs()
ReadAllItuserAddressesAddresses.update_forward_refs()
ReadAllItuserAddressesAddressesObjects.update_forward_refs()
ReadAllItuserAddressesAddressesPageInfo.update_forward_refs()
