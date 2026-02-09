from .base_model import BaseModel


class TestingSendEvent(BaseModel):
    event_send: bool


TestingSendEvent.update_forward_refs()
