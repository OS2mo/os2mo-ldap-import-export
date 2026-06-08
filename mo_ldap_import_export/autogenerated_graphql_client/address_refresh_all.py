from uuid import UUID

from .base_model import BaseModel


class AddressRefreshAll(BaseModel):
    address_refresh: "AddressRefreshAllAddressRefresh"


class AddressRefreshAllAddressRefresh(BaseModel):
    objects: list[UUID]


AddressRefreshAll.update_forward_refs()
AddressRefreshAllAddressRefresh.update_forward_refs()
