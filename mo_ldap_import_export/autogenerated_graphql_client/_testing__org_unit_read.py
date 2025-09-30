from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import Field

from .base_model import BaseModel


class TestingOrgUnitRead(BaseModel):
    org_units: "TestingOrgUnitReadOrgUnits"


class TestingOrgUnitReadOrgUnits(BaseModel):
    objects: list["TestingOrgUnitReadOrgUnitsObjects"]


class TestingOrgUnitReadOrgUnitsObjects(BaseModel):
    validities: list["TestingOrgUnitReadOrgUnitsObjectsValidities"]


class TestingOrgUnitReadOrgUnitsObjectsValidities(BaseModel):
    uuid: UUID
    user_key: str
    name: str
    parent: Optional["TestingOrgUnitReadOrgUnitsObjectsValiditiesParent"]
    unit_type: Optional["TestingOrgUnitReadOrgUnitsObjectsValiditiesUnitType"]
    org_unit_level: Optional["TestingOrgUnitReadOrgUnitsObjectsValiditiesOrgUnitLevel"]
    validity: "TestingOrgUnitReadOrgUnitsObjectsValiditiesValidity"


class TestingOrgUnitReadOrgUnitsObjectsValiditiesParent(BaseModel):
    uuid: UUID


class TestingOrgUnitReadOrgUnitsObjectsValiditiesUnitType(BaseModel):
    user_key: str


class TestingOrgUnitReadOrgUnitsObjectsValiditiesOrgUnitLevel(BaseModel):
    user_key: str


class TestingOrgUnitReadOrgUnitsObjectsValiditiesValidity(BaseModel):
    from_: datetime = Field(alias="from")
    to: datetime | None


TestingOrgUnitRead.update_forward_refs()
TestingOrgUnitReadOrgUnits.update_forward_refs()
TestingOrgUnitReadOrgUnitsObjects.update_forward_refs()
TestingOrgUnitReadOrgUnitsObjectsValidities.update_forward_refs()
TestingOrgUnitReadOrgUnitsObjectsValiditiesParent.update_forward_refs()
TestingOrgUnitReadOrgUnitsObjectsValiditiesUnitType.update_forward_refs()
TestingOrgUnitReadOrgUnitsObjectsValiditiesOrgUnitLevel.update_forward_refs()
TestingOrgUnitReadOrgUnitsObjectsValiditiesValidity.update_forward_refs()
