import httpx
import pytest

from transform_service.app.config import Settings
from transform_service.app.pipeline.pipeline import run_pipeline


@pytest.mark.asyncio
async def test_run_pipeline_order() -> None:
    settings = Settings()
    raw = {
        "event_id": "p1",
        "source": "src",
        "event_type": "ORDER.Created",
        "timestamp": "2024-03-01T10:00:00Z",
        "payload": {"x": 1},
        "metadata": {"Region": " EU "},
    }
    normalized, enrichments = await run_pipeline(raw, settings=settings, http_client=None)
    assert normalized["event_type"] == "order.created"
    assert enrichments["timestamp_normalized"] == "2024-03-01T10:00:00+00:00"
    assert normalized["metadata"]["region"] == "EU"


@pytest.mark.asyncio
async def test_run_pipeline_with_mock_geo() -> None:
    settings = Settings()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"status": "success", "country": "X", "countryCode": "XX", "city": "Y"},
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        raw = {
            "event_id": "p2",
            "source": "s",
            "event_type": "t",
            "timestamp": "2024-01-01T00:00:00+00:00",
            "payload": {"client_ip": "9.9.9.9"},
            "metadata": {},
        }
        normalized, enrichments = await run_pipeline(raw, settings=settings, http_client=client)
    assert enrichments["geo"]["country"] == "X"
    assert normalized["event_id"] == "p2"
