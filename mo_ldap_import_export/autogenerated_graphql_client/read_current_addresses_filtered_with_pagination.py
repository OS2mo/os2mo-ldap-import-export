from typing import Any
from typing import Optional
from uuid import UUID

from .base_model import BaseModel


class ReadCurrentAddressesFilteredWithPagination(BaseModel):
    addresses: "ReadCurrentAddressesFilteredWithPaginationAddresses"


class ReadCurrentAddressesFilteredWithPaginationAddresses(BaseModel):
    objects: list["ReadCurrentAddressesFilteredWithPaginationAddressesObjects"]
    page_info: "ReadCurrentAddressesFilteredWithPaginationAddressesPageInfo"


class ReadCurrentAddressesFilteredWithPaginationAddressesObjects(BaseModel):
    current: Optional[
        "ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrent"
    ]


class ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrent(BaseModel):
    address_type_response: "ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentAddressTypeResponse"
    engagement_response: Optional[
        "ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentEngagementResponse"
    ]
    person_response: Optional[
        "ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentPersonResponse"
    ]
    uuid: UUID
    user_key: str
    value: str
    visibility_response: Optional[
        "ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentVisibilityResponse"
    ]
    ituser_response: Optional[
        "ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentItuserResponse"
    ]


class ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentAddressTypeResponse(
    BaseModel
):
    current: Optional[
        "ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentAddressTypeResponseCurrent"
    ]


class ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentAddressTypeResponseCurrent(
    BaseModel
):
    uuid: UUID
    user_key: str
    name: str


class ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentEngagementResponse(
    BaseModel
):
    current: Optional[
        "ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentEngagementResponseCurrent"
    ]


class ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentEngagementResponseCurrent(
    BaseModel
):
    uuid: UUID
    user_key: str


class ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentPersonResponse(
    BaseModel
):
    current: Optional[
        "ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentPersonResponseCurrent"
    ]


class ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentPersonResponseCurrent(
    BaseModel
):
    uuid: UUID
    user_key: str
    name: str


class ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentVisibilityResponse(
    BaseModel
):
    current: Optional[
        "ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentVisibilityResponseCurrent"
    ]


class ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentVisibilityResponseCurrent(
    BaseModel
):
    uuid: UUID
    user_key: str
    description: str | None
    name: str
    facet_response: "ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentVisibilityResponseCurrentFacetResponse"


class ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentVisibilityResponseCurrentFacetResponse(
    BaseModel
):
    uuid: UUID


class ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentItuserResponse(
    BaseModel
):
    current: Optional[
        "ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentItuserResponseCurrent"
    ]


class ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentItuserResponseCurrent(
    BaseModel
):
    uuid: UUID
    user_key: str


class ReadCurrentAddressesFilteredWithPaginationAddressesPageInfo(BaseModel):
    next_cursor: Any | None


ReadCurrentAddressesFilteredWithPagination.update_forward_refs()
ReadCurrentAddressesFilteredWithPaginationAddresses.update_forward_refs()
ReadCurrentAddressesFilteredWithPaginationAddressesObjects.update_forward_refs()
ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrent.update_forward_refs()
ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentAddressTypeResponse.update_forward_refs()
ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentAddressTypeResponseCurrent.update_forward_refs()
ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentEngagementResponse.update_forward_refs()
ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentEngagementResponseCurrent.update_forward_refs()
ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentPersonResponse.update_forward_refs()
ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentPersonResponseCurrent.update_forward_refs()
ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentVisibilityResponse.update_forward_refs()
ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentVisibilityResponseCurrent.update_forward_refs()
ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentVisibilityResponseCurrentFacetResponse.update_forward_refs()
ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentItuserResponse.update_forward_refs()
ReadCurrentAddressesFilteredWithPaginationAddressesObjectsCurrentItuserResponseCurrent.update_forward_refs()
ReadCurrentAddressesFilteredWithPaginationAddressesPageInfo.update_forward_refs()
