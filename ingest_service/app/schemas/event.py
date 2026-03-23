from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class EventIngest(BaseModel):
    source: str = Field(..., min_length=1)
    event_type: str = Field(..., min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, str] = Field(default_factory=dict)


class EventIngestResponse(BaseModel):
    event_id: UUID
    status: str = "accepted"


class BatchEventIngest(BaseModel):
    events: list[EventIngest] = Field(..., min_length=1)


class BatchEventIngestResponse(BaseModel):
    count: int
    event_ids: list[UUID]
    status: str = "accepted"
