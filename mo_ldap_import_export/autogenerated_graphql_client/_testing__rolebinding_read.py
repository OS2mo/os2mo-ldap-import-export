from datetime import datetime
from uuid import UUID

from pydantic import Field

from .base_model import BaseModel


class TestingRolebindingRead(BaseModel):
    rolebindings: "TestingRolebindingReadRolebindings"


class TestingRolebindingReadRolebindings(BaseModel):
    objects: list["TestingRolebindingReadRolebindingsObjects"]


class TestingRolebindingReadRolebindingsObjects(BaseModel):
    validities: list["TestingRolebindingReadRolebindingsObjectsValidities"]


class TestingRolebindingReadRolebindingsObjectsValidities(BaseModel):
    uuid: UUID
    user_key: str
    validity: "TestingRolebindingReadRolebindingsObjectsValiditiesValidity"
    ituser: list["TestingRolebindingReadRolebindingsObjectsValiditiesItuser"]
    org_unit: list["TestingRolebindingReadRolebindingsObjectsValiditiesOrgUnit"]
    role: list["TestingRolebindingReadRolebindingsObjectsValiditiesRole"]


class TestingRolebindingReadRolebindingsObjectsValiditiesValidity(BaseModel):
    from_: datetime = Field(alias="from")
    to: datetime | None


class TestingRolebindingReadRolebindingsObjectsValiditiesItuser(BaseModel):
    uuid: UUID


class TestingRolebindingReadRolebindingsObjectsValiditiesOrgUnit(BaseModel):
    uuid: UUID


class TestingRolebindingReadRolebindingsObjectsValiditiesRole(BaseModel):
    uuid: UUID


TestingRolebindingRead.update_forward_refs()
TestingRolebindingReadRolebindings.update_forward_refs()
TestingRolebindingReadRolebindingsObjects.update_forward_refs()
TestingRolebindingReadRolebindingsObjectsValidities.update_forward_refs()
TestingRolebindingReadRolebindingsObjectsValiditiesValidity.update_forward_refs()
TestingRolebindingReadRolebindingsObjectsValiditiesItuser.update_forward_refs()
TestingRolebindingReadRolebindingsObjectsValiditiesOrgUnit.update_forward_refs()
TestingRolebindingReadRolebindingsObjectsValiditiesRole.update_forward_refs()
