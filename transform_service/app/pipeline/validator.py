from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class RawEventSchema(BaseModel):
    model_config = ConfigDict(extra="allow")

    event_id: str
    source: str
    event_type: str
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
