from typing import List
from uuid import UUID

from .base_model import BaseModel


class TestingCreateEmployee(BaseModel):
    employee_create: "TestingCreateEmployeeEmployeeCreate"


class TestingCreateEmployeeEmployeeCreate(BaseModel):
    validities: List["TestingCreateEmployeeEmployeeCreateValidities"]


class TestingCreateEmployeeEmployeeCreateValidities(BaseModel):
    uuid: UUID


TestingCreateEmployee.update_forward_refs()
TestingCreateEmployeeEmployeeCreate.update_forward_refs()
TestingCreateEmployeeEmployeeCreateValidities.update_forward_refs()
