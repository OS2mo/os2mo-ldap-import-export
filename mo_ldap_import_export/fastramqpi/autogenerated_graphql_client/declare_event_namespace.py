from .base_model import BaseModel


class DeclareEventNamespace(BaseModel):
    event_namespace_declare: "DeclareEventNamespaceEventNamespaceDeclare"


class DeclareEventNamespaceEventNamespaceDeclare(BaseModel):
    name: str


DeclareEventNamespace.update_forward_refs()
DeclareEventNamespaceEventNamespaceDeclare.update_forward_refs()
