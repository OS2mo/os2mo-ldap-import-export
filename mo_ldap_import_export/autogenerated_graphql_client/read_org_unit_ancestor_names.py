from typing import List
from typing import Optional

from .base_model import BaseModel


class ReadOrgUnitAncestorNames(BaseModel):
    org_units: "ReadOrgUnitAncestorNamesOrgUnits"


class ReadOrgUnitAncestorNamesOrgUnits(BaseModel):
    objects: List["ReadOrgUnitAncestorNamesOrgUnitsObjects"]


class ReadOrgUnitAncestorNamesOrgUnitsObjects(BaseModel):
    current: Optional["ReadOrgUnitAncestorNamesOrgUnitsObjectsCurrent"]


class ReadOrgUnitAncestorNamesOrgUnitsObjectsCurrent(BaseModel):
    name: str
    ancestors: List["ReadOrgUnitAncestorNamesOrgUnitsObjectsCurrentAncestors"]


class ReadOrgUnitAncestorNamesOrgUnitsObjectsCurrentAncestors(BaseModel):
    name: str


ReadOrgUnitAncestorNames.update_forward_refs()
ReadOrgUnitAncestorNamesOrgUnits.update_forward_refs()
ReadOrgUnitAncestorNamesOrgUnitsObjects.update_forward_refs()
ReadOrgUnitAncestorNamesOrgUnitsObjectsCurrent.update_forward_refs()
ReadOrgUnitAncestorNamesOrgUnitsObjectsCurrentAncestors.update_forward_refs()
