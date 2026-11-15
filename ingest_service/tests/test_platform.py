import json
from uuid import UUID

import grpc
import httpx
import pytest
from fastapi import HTTPException

from eventpipe.platform import authorize_tenant
from ingest_service.app.config import Settings, get_settings
from ingest_service.app.generated import event_pb2, event_pb2_grpc
from ingest_service.app.grpc_server.servicer import EventServiceServicer
from ingest_service.app.metrics import events_ingested_total
from ingest_service.tests.conftest import FakeKafkaProducer

TENANT = "00000000-0000-4000-8000-000000000001"
USER = "00000000-0000-4000-8000-000000000002"
HEADERS = {"authorization": "Bearer fixture-only", "x-tenant-id": TENANT}


def mock_identity(monkeypatch, handler):
    client_type = httpx.AsyncClient
    monkeypatch.setattr("eventpipe.platform.httpx.AsyncClient", lambda **kwargs: client_type(
        transport=httpx.MockTransport(handler), **kwargs,
    ))


def allowed(request):
    assert request.headers["authorization"] == HEADERS["authorization"]
    permission = json.loads(request.content)["permission"]
    return httpx.Response(200, json={
        "allowed": True, "tenant_id": TENANT, "user_id": USER,
        "role": "owner", "permission": permission,
    })


@pytest.mark.parametrize("status", [401, 403, 404, 429, 500, 302])
async def test_identity_rejection_fails_closed(monkeypatch, status):
    mock_identity(monkeypatch, lambda _: httpx.Response(status, json={"detail": "private input"}))
    with pytest.raises(HTTPException) as failure:
        await authorize_tenant(platform_mode=True, auth_url="http://identity", permission="tenant.read",
                               authorization=HEADERS["authorization"], tenant_selector=TENANT)
    assert failure.value.status_code == (status if status in (401, 403, 404) else 503)
    assert "private input" not in failure.value.detail


@pytest.mark.parametrize("data", [None, [], {}, {"allowed": True}, {
    "allowed": True, "tenant_id": USER, "user_id": USER, "role": "owner", "permission": "tenant.read",
}, {"allowed": True, "tenant_id": TENANT, "user_id": USER, "role": "member", "permission": "tenant.manage"}])
async def test_invalid_identity_context_fails_closed(monkeypatch, data):
    mock_identity(monkeypatch, lambda _: httpx.Response(200, json=data))
    with pytest.raises(HTTPException) as failure:
        await authorize_tenant(platform_mode=True, auth_url="http://identity", permission="tenant.manage",
                               authorization=HEADERS["authorization"], tenant_selector=TENANT)
    assert failure.value.status_code == 503


async def test_identity_timeout_and_no_cache(monkeypatch):
    count = 0

    def handler(request):
        nonlocal count
        count += 1
        if count == 2:
            raise httpx.ReadTimeout("private bearer detail", request=request)
        return allowed(request)

    mock_identity(monkeypatch, handler)
    kwargs = {"platform_mode": True, "auth_url": "http://identity", "permission": "tenant.manage",
              "authorization": HEADERS["authorization"], "tenant_selector": TENANT}
    assert await authorize_tenant(**kwargs) == UUID(TENANT)
    with pytest.raises(HTTPException) as failure:
        await authorize_tenant(**kwargs)
    assert failure.value.status_code == 503 and count == 2
    assert "private bearer" not in failure.value.detail


@pytest.mark.parametrize("authorization", [None, "Basic fixture", "Bearer ", "Bearer ü", "Bearer x\r\n", "Bearer " + "a" * 8192])
async def test_invalid_bearer_never_calls_identity(authorization):
    with pytest.raises(HTTPException) as failure:
        await authorize_tenant(platform_mode=True, auth_url="", permission="tenant.read",
                               authorization=authorization, tenant_selector=TENANT)
    assert failure.value.status_code == 401


async def test_platform_rest_and_batch_use_only_authorized_context(rest_client, monkeypatch):
    client, producer = rest_client
    monkeypatch.setattr("ingest_service.app.api.events.get_settings", lambda: Settings(platform_mode=True, auth_url="http://identity"))
    mock_identity(monkeypatch, allowed)
    event = {"source": "source", "event_type": "event", "payload": {"tenant_id": USER}}
    denied = await client.post("/api/v1/events", json=event)
    assert denied.status_code == 401 and producer.published == []
    accepted = await client.post("/api/v1/events", headers=HEADERS, json=event)
    assert accepted.status_code == 200
    assert producer.published[0][1]["tenant_id"] == TENANT
    forged = await client.post("/api/v1/events", headers=HEADERS, json={**event, "tenant_id": USER})
    assert forged.status_code == 422
    batch = await client.post("/api/v1/events/batch", headers=HEADERS, json={"events": [event, event]})
    assert batch.status_code == 200 and len(producer.published) == 3
    assert all(body["tenant_id"] == TENANT for _, body in producer.published)
    assert events_ingested_total._labelnames == ()


async def test_rest_size_and_batch_bounds(rest_client):
    client, producer = rest_client
    event = {"source": "s", "event_type": "t"}
    assert (await client.post("/api/v1/events", content=b" " * (1024 * 1024 + 1))).status_code == 413
    assert (await client.post("/api/v1/events/batch", json={"events": [event] * 1001})).status_code == 422
    assert producer.published == []


async def test_grpc_platform_unary_stream_revocation_and_forged_id(monkeypatch):
    producer = FakeKafkaProducer()
    server = grpc.aio.server()
    event_pb2_grpc.add_EventServiceServicer_to_server(
        EventServiceServicer(producer, Settings(platform_mode=True, auth_url="http://identity")), server,
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        return httpx.Response(401) if calls >= 4 else allowed(request)

    mock_identity(monkeypatch, handler)
    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = event_pb2_grpc.EventServiceStub(channel)
            event = event_pb2.Event(event_id=USER, source="s", event_type="t")
            with pytest.raises(grpc.aio.AioRpcError) as denied:
                await stub.Ingest(event)
            assert denied.value.code() == grpc.StatusCode.UNAUTHENTICATED
            assert producer.published == []
            result = await stub.Ingest(event, metadata=tuple(HEADERS.items()))
            assert result.event_id != USER and producer.published[0][1]["tenant_id"] == TENANT

            async def stream():
                yield event
                yield event

            with pytest.raises(grpc.aio.AioRpcError) as revoked:
                await stub.IngestStream(stream(), metadata=tuple(HEADERS.items()))
            assert revoked.value.code() == grpc.StatusCode.UNAUTHENTICATED
            assert len(producer.published) == 2 and calls == 4
    finally:
        await server.stop(0)


def test_settings_default_to_standalone(monkeypatch):
    monkeypatch.delenv("EVENTPIPE_PLATFORM_MODE", raising=False)
    get_settings.cache_clear()
    assert not get_settings().platform_mode


async def test_broker_outage_returns_sanitized_503(rest_client, monkeypatch):
    from unittest.mock import AsyncMock

    client, producer = rest_client
    producer.publish_raw_event = AsyncMock(side_effect=TimeoutError("sensitive-fixture-only"))
    response = await client.post("/api/v1/events", json={"source": "s", "event_type": "t"})
    assert response.status_code == 503
    assert response.json() == {"detail": "event broker unavailable"}
