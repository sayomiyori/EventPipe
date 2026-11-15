from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class RawEventSchema(BaseModel):
    model_config = ConfigDict(extra="allow")

    event_id: str = Field(max_length=64)
    source: str = Field(max_length=255)
    event_type: str = Field(max_length=255)
    tenant_id: UUID | None = None
    timestamp: str
    payload: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("event_id", "source", "event_type")
    @classmethod
    def non_empty_str(cls, v: str) -> str:
        s = (v or "").strip()
        if not s:
            raise ValueError("must be non-empty")
        return s


def validate_raw_event(data: dict[str, Any]) -> RawEventSchema:
    return RawEventSchema.model_validate(data)


def validated_to_dict(model: RawEventSchema) -> dict[str, Any]:
    return model.model_dump()
