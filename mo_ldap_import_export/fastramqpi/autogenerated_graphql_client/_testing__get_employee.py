from typing import Any
from typing import List
from typing import Optional
from uuid import UUID

from .base_model import BaseModel


class TestingGetEmployee(BaseModel):
    employees: "TestingGetEmployeeEmployees"


class TestingGetEmployeeEmployees(BaseModel):
    objects: List["TestingGetEmployeeEmployeesObjects"]


class TestingGetEmployeeEmployeesObjects(BaseModel):
    validities: List["TestingGetEmployeeEmployeesObjectsValidities"]


class TestingGetEmployeeEmployeesObjectsValidities(BaseModel):
    uuid: UUID
    cpr_number: Optional[Any]
    given_name: str


TestingGetEmployee.update_forward_refs()
TestingGetEmployeeEmployees.update_forward_refs()
TestingGetEmployeeEmployeesObjects.update_forward_refs()
TestingGetEmployeeEmployeesObjectsValidities.update_forward_refs()
