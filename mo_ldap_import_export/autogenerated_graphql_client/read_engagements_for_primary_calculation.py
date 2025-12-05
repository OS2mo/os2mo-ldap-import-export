from datetime import datetime
from uuid import UUID

from pydantic import Field

from .base_model import BaseModel


class ReadEngagementsForPrimaryCalculation(BaseModel):
    engagements: "ReadEngagementsForPrimaryCalculationEngagements"


class ReadEngagementsForPrimaryCalculationEngagements(BaseModel):
    objects: list["ReadEngagementsForPrimaryCalculationEngagementsObjects"]


class ReadEngagementsForPrimaryCalculationEngagementsObjects(BaseModel):
    validities: list["ReadEngagementsForPrimaryCalculationEngagementsObjectsValidities"]


class ReadEngagementsForPrimaryCalculationEngagementsObjectsValidities(BaseModel):
    fraction: int | None
    user_key: str
    uuid: UUID
    validity: "ReadEngagementsForPrimaryCalculationEngagementsObjectsValiditiesValidity"
    engagement_type: (
        "ReadEngagementsForPrimaryCalculationEngagementsObjectsValiditiesEngagementType"
    )


class ReadEngagementsForPrimaryCalculationEngagementsObjectsValiditiesValidity(
    BaseModel
):
    from_: datetime = Field(alias="from")
    to: datetime | None


class ReadEngagementsForPrimaryCalculationEngagementsObjectsValiditiesEngagementType(
    BaseModel
):
    uuid: UUID


ReadEngagementsForPrimaryCalculation.update_forward_refs()
ReadEngagementsForPrimaryCalculationEngagements.update_forward_refs()
ReadEngagementsForPrimaryCalculationEngagementsObjects.update_forward_refs()
ReadEngagementsForPrimaryCalculationEngagementsObjectsValidities.update_forward_refs()
ReadEngagementsForPrimaryCalculationEngagementsObjectsValiditiesValidity.update_forward_refs()
ReadEngagementsForPrimaryCalculationEngagementsObjectsValiditiesEngagementType.update_forward_refs()
