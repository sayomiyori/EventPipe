import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import structlog
from aiokafka.structs import TopicPartition

from transform_service.app.config import Settings
from transform_service.app.consumer import process_kafka_message, run_consumer_loop


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [b"\xff", b"[]", b"null", b"{bad-json"])
async def test_malformed_message_is_delivered_to_dlq(raw):
    producer = SimpleNamespace(send_and_wait=AsyncMock())
    msg = SimpleNamespace(value=raw, key=b"event", topic="events.raw", partition=0, offset=0)
    await process_kafka_message(
        msg,
        settings=Settings(),
        session_factory=None,
        s3_session=None,
        http_client=None,
        dlq_producer=producer,
        log=structlog.get_logger(),
    )
    body = producer.send_and_wait.call_args.kwargs["value"]
    assert body["error"]
    assert "original" in body


@pytest.mark.asyncio
async def test_failed_dlq_delivery_does_not_commit_source_offset(monkeypatch):
    tp = TopicPartition("events.raw", 0)
    message = SimpleNamespace(value=b"{bad-json", key=b"event", topic=tp.topic, partition=0, offset=5)
    consumer = SimpleNamespace(
        start=AsyncMock(), stop=AsyncMock(), getmany=AsyncMock(return_value={tp: [message]}),
        highwater=lambda _: 6, commit=AsyncMock(),
    )
    producer = SimpleNamespace(
        start=AsyncMock(), stop=AsyncMock(), send_and_wait=AsyncMock(side_effect=RuntimeError("DLQ unavailable"))
    )
    monkeypatch.setattr("transform_service.app.consumer.AIOKafkaConsumer", lambda *a, **kw: consumer)
    monkeypatch.setattr("transform_service.app.consumer.AIOKafkaProducer", lambda *a, **kw: producer)
    with pytest.raises(RuntimeError, match="DLQ unavailable"):
        await run_consumer_loop(Settings(), None, asyncio.Event())
    assert consumer.commit.await_count == 0


@pytest.mark.asyncio
async def test_successful_dlq_delivery_commits_source_offset(monkeypatch):
    tp = TopicPartition("events.raw", 0)
    message = SimpleNamespace(value=json.dumps({"source": "missing-required-fields"}).encode(),
                              key=b"event", topic=tp.topic, partition=0, offset=5)
    stop = asyncio.Event()

    async def commit(offsets):
        assert offsets[tp].offset == 6
        stop.set()

    consumer = SimpleNamespace(
        start=AsyncMock(), stop=AsyncMock(), getmany=AsyncMock(return_value={tp: [message]}),
        highwater=lambda _: 6, commit=AsyncMock(side_effect=commit),
    )
    producer = SimpleNamespace(start=AsyncMock(), stop=AsyncMock(), send_and_wait=AsyncMock())
    monkeypatch.setattr("transform_service.app.consumer.AIOKafkaConsumer", lambda *a, **kw: consumer)
    monkeypatch.setattr("transform_service.app.consumer.AIOKafkaProducer", lambda *a, **kw: producer)
    await run_consumer_loop(Settings(max_retries=1), None, stop)
    assert consumer.commit.await_count == 1
    body = producer.send_and_wait.call_args.kwargs["value"]
    assert body["original"]["offset"] == 5
    assert body["original"]["partition"] == 0
    assert "missing-required-fields" not in json.dumps(body)
    assert "raw" not in body["original"]


@pytest.mark.asyncio
async def test_storage_outage_escapes_without_dlq_or_sensitive_logs(monkeypatch):
    raw = {"event_id": "e", "source": "s", "event_type": "t", "timestamp": "2026-10-11T00:00:00Z",
           "payload": {"password": "sensitive-fixture-only"}}
    session = SimpleNamespace(execute=AsyncMock(side_effect=OSError("sensitive-fixture-only")))

    class Context:
        async def __aenter__(self):
            return session

        async def __aexit__(self, *args):
            pass

    producer = SimpleNamespace(send_and_wait=AsyncMock())
    log = MagicMock()
    with pytest.raises(RuntimeError, match="infrastructure unavailable"):
        await process_kafka_message(
            SimpleNamespace(value=json.dumps(raw).encode(), key=b"sensitive-fixture-only",
                            topic="events.raw", partition=0, offset=0),
            settings=Settings(max_retries=1), session_factory=Context, s3_session=None,
            http_client=None, dlq_producer=producer, log=log,
        )
    assert producer.send_and_wait.await_count == 0
    assert "sensitive-fixture-only" not in str(log.mock_calls)


@pytest.mark.asyncio
async def test_validation_dlq_has_no_original_secrets_or_kafka_key():
    raw = {"event_id": "sensitive-fixture-only", "source": "",
           "payload": {"token": "sensitive-fixture-only"}}
    producer = SimpleNamespace(send_and_wait=AsyncMock())
    log = MagicMock()
    await process_kafka_message(
        SimpleNamespace(value=json.dumps(raw).encode(), key=b"sensitive-fixture-only",
                        topic="events.raw", partition=0, offset=1),
        settings=Settings(max_retries=1), session_factory=None, s3_session=None,
        http_client=None, dlq_producer=producer, log=log,
    )
    assert "sensitive-fixture-only" not in json.dumps(producer.send_and_wait.call_args.kwargs)
    assert "sensitive-fixture-only" not in str(log.mock_calls)
