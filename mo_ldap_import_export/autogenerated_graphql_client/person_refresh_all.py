from uuid import UUID

from .base_model import BaseModel


class PersonRefreshAll(BaseModel):
    employee_refresh: "PersonRefreshAllEmployeeRefresh"


class PersonRefreshAllEmployeeRefresh(BaseModel):
    objects: list[UUID]


PersonRefreshAll.update_forward_refs()
PersonRefreshAllEmployeeRefresh.update_forward_refs()
