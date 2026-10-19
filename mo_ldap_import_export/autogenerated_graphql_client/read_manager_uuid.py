from uuid import UUID

from .base_model import BaseModel


class ReadManagerUuid(BaseModel):
    managers: "ReadManagerUuidManagers"


class ReadManagerUuidManagers(BaseModel):
    objects: list["ReadManagerUuidManagersObjects"]


class ReadManagerUuidManagersObjects(BaseModel):
    uuid: UUID


ReadManagerUuid.update_forward_refs()
ReadManagerUuidManagers.update_forward_refs()
ReadManagerUuidManagersObjects.update_forward_refs()
