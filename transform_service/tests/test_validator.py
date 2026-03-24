import pytest
from pydantic import ValidationError

from transform_service.app.pipeline.validator import validate_raw_event


def test_validate_raw_event_ok() -> None:
    data = {
        "event_id": "e1",
        "source": "svc",
        "event_type": "click",
        "timestamp": "2024-01-01T00:00:00+00:00",
        "payload": {"a": 1},
        "metadata": {"k": "v"},
    }
    m = validate_raw_event(data)
    assert m.event_id == "e1"
    assert m.event_type == "click"


def test_validate_rejects_empty_event_type() -> None:
    with pytest.raises(ValidationError):
        validate_raw_event(
            {
                "event_id": "e",
                "source": "s",
                "event_type": "  ",
                "timestamp": "2024-01-01T00:00:00+00:00",
            }
        )
