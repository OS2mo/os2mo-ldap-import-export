from typing import Any
from typing import Optional

from .base_model import BaseModel


class FetchEvent(BaseModel):
    event_fetch: Optional["FetchEventEventFetch"]


class FetchEventEventFetch(BaseModel):
    subject: str
    priority: int
    token: Any


FetchEvent.update_forward_refs()
FetchEventEventFetch.update_forward_refs()
