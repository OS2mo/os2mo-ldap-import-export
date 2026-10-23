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
    address_type_response: (
        "ReadOrphanedAddressesAddressesObjectsCurrentAddressTypeResponse"
    )
    uuid: UUID
    user_key: str
    value: str
    visibility_response: Optional[
        "ReadOrphanedAddressesAddressesObjectsCurrentVisibilityResponse"
    ]
    ituser_response: Optional[
        "ReadOrphanedAddressesAddressesObjectsCurrentItuserResponse"
    ]


class ReadOrphanedAddressesAddressesObjectsCurrentAddressTypeResponse(BaseModel):
    current: Optional[
        "ReadOrphanedAddressesAddressesObjectsCurrentAddressTypeResponseCurrent"
    ]


class ReadOrphanedAddressesAddressesObjectsCurrentAddressTypeResponseCurrent(BaseModel):
    uuid: UUID
    user_key: str
    name: str


class ReadOrphanedAddressesAddressesObjectsCurrentVisibilityResponse(BaseModel):
    current: Optional[
        "ReadOrphanedAddressesAddressesObjectsCurrentVisibilityResponseCurrent"
    ]


class ReadOrphanedAddressesAddressesObjectsCurrentVisibilityResponseCurrent(BaseModel):
    uuid: UUID
    user_key: str
    description: str | None
    name: str
    facet_response: "ReadOrphanedAddressesAddressesObjectsCurrentVisibilityResponseCurrentFacetResponse"


class ReadOrphanedAddressesAddressesObjectsCurrentVisibilityResponseCurrentFacetResponse(
    BaseModel
):
    uuid: UUID


class ReadOrphanedAddressesAddressesObjectsCurrentItuserResponse(BaseModel):
    current: Optional[
        "ReadOrphanedAddressesAddressesObjectsCurrentItuserResponseCurrent"
    ]


class ReadOrphanedAddressesAddressesObjectsCurrentItuserResponseCurrent(BaseModel):
    uuid: UUID
    user_key: str
    external_id: str | None
    engagements: list[
        "ReadOrphanedAddressesAddressesObjectsCurrentItuserResponseCurrentEngagements"
    ]


class ReadOrphanedAddressesAddressesObjectsCurrentItuserResponseCurrentEngagements(
    BaseModel
):
    current: Optional[
        "ReadOrphanedAddressesAddressesObjectsCurrentItuserResponseCurrentEngagementsCurrent"
    ]


class ReadOrphanedAddressesAddressesObjectsCurrentItuserResponseCurrentEngagementsCurrent(
    BaseModel
):
    uuid: UUID
    user_key: str


class ReadOrphanedAddressesAddressesPageInfo(BaseModel):
    next_cursor: Any | None


ReadOrphanedAddresses.update_forward_refs()
ReadOrphanedAddressesAddresses.update_forward_refs()
ReadOrphanedAddressesAddressesObjects.update_forward_refs()
ReadOrphanedAddressesAddressesObjectsCurrent.update_forward_refs()
ReadOrphanedAddressesAddressesObjectsCurrentAddressTypeResponse.update_forward_refs()
ReadOrphanedAddressesAddressesObjectsCurrentAddressTypeResponseCurrent.update_forward_refs()
ReadOrphanedAddressesAddressesObjectsCurrentVisibilityResponse.update_forward_refs()
ReadOrphanedAddressesAddressesObjectsCurrentVisibilityResponseCurrent.update_forward_refs()
ReadOrphanedAddressesAddressesObjectsCurrentVisibilityResponseCurrentFacetResponse.update_forward_refs()
ReadOrphanedAddressesAddressesObjectsCurrentItuserResponse.update_forward_refs()
ReadOrphanedAddressesAddressesObjectsCurrentItuserResponseCurrent.update_forward_refs()
ReadOrphanedAddressesAddressesObjectsCurrentItuserResponseCurrentEngagements.update_forward_refs()
ReadOrphanedAddressesAddressesObjectsCurrentItuserResponseCurrentEngagementsCurrent.update_forward_refs()
ReadOrphanedAddressesAddressesPageInfo.update_forward_refs()
