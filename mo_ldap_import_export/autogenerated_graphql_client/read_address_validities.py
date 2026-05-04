from datetime import datetime
from uuid import UUID

from pydantic import Field

from .base_model import BaseModel


class ReadAddressValidities(BaseModel):
    addresses: "ReadAddressValiditiesAddresses"


class ReadAddressValiditiesAddresses(BaseModel):
    objects: list["ReadAddressValiditiesAddressesObjects"]


class ReadAddressValiditiesAddressesObjects(BaseModel):
    validities: list["ReadAddressValiditiesAddressesObjectsValidities"]


class ReadAddressValiditiesAddressesObjectsValidities(BaseModel):
    uuid: UUID
    user_key: str
    value: str
    visibility_uuid: UUID | None
    employee_uuid: UUID | None
    org_unit_uuid: UUID | None
    engagement_uuid: UUID | None
    ituser_uuid: UUID | None
    validity: "ReadAddressValiditiesAddressesObjectsValiditiesValidity"
    address_type: "ReadAddressValiditiesAddressesObjectsValiditiesAddressType"


class ReadAddressValiditiesAddressesObjectsValiditiesValidity(BaseModel):
    from_: datetime = Field(alias="from")
    to: datetime | None


class ReadAddressValiditiesAddressesObjectsValiditiesAddressType(BaseModel):
    uuid: UUID


ReadAddressValidities.update_forward_refs()
ReadAddressValiditiesAddresses.update_forward_refs()
ReadAddressValiditiesAddressesObjects.update_forward_refs()
ReadAddressValiditiesAddressesObjectsValidities.update_forward_refs()
ReadAddressValiditiesAddressesObjectsValiditiesValidity.update_forward_refs()
ReadAddressValiditiesAddressesObjectsValiditiesAddressType.update_forward_refs()
