from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class EventIngest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source: str = Field(..., min_length=1, max_length=255)
    event_type: str = Field(..., min_length=1, max_length=255)
    payload: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, str] = Field(default_factory=dict)


class EventIngestResponse(BaseModel):
    event_id: UUID
    status: str = "accepted"


class BatchEventIngest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    events: list[EventIngest] = Field(..., min_length=1, max_length=1000)


class BatchEventIngestResponse(BaseModel):
    count: int
    event_ids: list[UUID]
    status: str = "accepted"
