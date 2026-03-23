import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_post_event_accepted(rest_client: tuple[AsyncClient, object]) -> None:
    client, producer = rest_client
    resp = await client.post(
        "/api/v1/events",
        json={
            "source": "test",
            "event_type": "user.signup",
            "payload": {"user_id": 1},
            "metadata": {"env": "test"},
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "accepted"
    assert "event_id" in data
    assert len(producer.published) == 1
    key, body = producer.published[0]
    assert key == "user.signup"
    assert body["event_type"] == "user.signup"
    assert body["source"] == "test"
    assert body["payload"] == {"user_id": 1}
    assert body["metadata"] == {"env": "test"}
    assert body["event_id"] == data["event_id"]


@pytest.mark.asyncio
async def test_post_event_validation(rest_client: tuple[AsyncClient, object]) -> None:
    client, producer = rest_client
    resp = await client.post(
        "/api/v1/events",
        json={"source": "", "event_type": "x", "payload": {}},
    )
    assert resp.status_code == 422
    assert producer.published == []


@pytest.mark.asyncio
async def test_post_batch(rest_client: tuple[AsyncClient, object]) -> None:
    client, producer = rest_client
    resp = await client.post(
        "/api/v1/events/batch",
        json={
            "events": [
                {"source": "a", "event_type": "t1", "payload": {"n": 1}},
                {"source": "b", "event_type": "t2", "payload": {"n": 2}},
            ]
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "accepted"
    assert data["count"] == 2
    assert len(data["event_ids"]) == 2
    assert len(producer.published) == 2
    assert producer.published[0][0] == "t1"
    assert producer.published[1][0] == "t2"
