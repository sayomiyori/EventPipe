import httpx
import pytest

from transform_service.app.config import Settings
from transform_service.app.pipeline.enricher import enrich, lookup_geo_ip


@pytest.mark.asyncio
async def test_enrich_timestamp_only() -> None:
    settings = Settings()
    event = {
        "event_id": "1",
        "source": "s",
        "event_type": "t",
        "timestamp": "2024-06-15T12:30:00Z",
        "payload": {},
        "metadata": {},
    }
    out = await enrich(event, settings=settings, http_client=None)
    assert out["timestamp_normalized"] == "2024-06-15T12:30:00+00:00"
    assert out["geo"] is None


@pytest.mark.asyncio
async def test_enrich_geo_skipped_without_client() -> None:
    settings = Settings()
    event = {
        "event_id": "1",
        "source": "s",
        "event_type": "t",
        "timestamp": "2024-01-01T00:00:00+00:00",
        "payload": {"client_ip": "8.8.8.8"},
        "metadata": {},
    }
    out = await enrich(event, settings=settings, http_client=None)
    assert out["geo"] == {"skipped": True, "ip": "8.8.8.8"}


@pytest.mark.asyncio
async def test_lookup_geo_ip_mock_transport() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "8.8.8.8" in str(request.url)
        return httpx.Response(
            200,
            json={
                "status": "success",
                "country": "Testland",
                "countryCode": "TL",
                "city": "Test City",
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        geo = await lookup_geo_ip("8.8.8.8", client, 2.0)
    assert geo["country"] == "Testland"
    assert geo["country_code"] == "TL"


@pytest.mark.asyncio
async def test_enrich_with_geo_client() -> None:
    settings = Settings()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"status": "success", "country": "Narnia", "countryCode": "NR", "city": "Cair"},
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        event = {
            "event_id": "1",
            "source": "s",
            "event_type": "t",
            "timestamp": "2024-01-01T00:00:00+00:00",
            "payload": {"client_ip": "1.2.3.4"},
            "metadata": {},
        }
        out = await enrich(event, settings=settings, http_client=client)
    assert out["geo"]["country"] == "Narnia"
