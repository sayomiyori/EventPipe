import asyncio
import json
import os
import uuid

import pytest
from aiokafka import AIOKafkaConsumer
from httpx import ASGITransport, AsyncClient

from ingest_service.app.config import Settings
from ingest_service.app.kafka.producer import EventKafkaProducer
from ingest_service.app.main import create_app


def _bootstrap() -> str:
    return os.environ.get("EVENTPIPE_KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rest_to_kafka_json_roundtrip() -> None:
    bootstrap = _bootstrap()
    topic = os.environ.get("EVENTPIPE_KAFKA_TOPIC_RAW", "events.raw")
    unique = str(uuid.uuid4())

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=f"eventpipe-test-{unique}",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    try:
        await consumer.start()
    except Exception as exc:  # noqa: BLE001 — broker discovery / network
        try:
            await consumer.stop()
        except Exception:
            pass
        pytest.skip(f"Kafka not reachable at {bootstrap}: {exc}")

    try:
        for _ in range(100):
            if consumer.assignment():
                break
            await asyncio.sleep(0.2)
        else:
            pytest.fail("timed out waiting for partition assignment")

        for tp in consumer.assignment():
            await consumer.seek_to_end(tp)

        producer = EventKafkaProducer(
            Settings(
                kafka_bootstrap_servers=bootstrap,
                kafka_topic_raw=topic,
            )
        )
        await producer.start()
        application = create_app()
        application.state.producer = producer
        transport = ASGITransport(app=application)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/api/v1/events",
                json={
                    "source": "integration",
                    "event_type": "integration.probe",
                    "payload": {"token": unique},
                    "metadata": {},
                },
            )
        assert resp.status_code == 200
        event_id = resp.json()["event_id"]

        found = None
        for _ in range(60):
            batch = await consumer.getmany(timeout_ms=2000, max_records=20)
            for _tp, records in batch.items():
                for rec in records:
                    v = rec.value
                    if isinstance(v, dict) and v.get("payload", {}).get("token") == unique:
                        found = rec
                        break
                if found:
                    break
            if found:
                break
        assert found is not None
        assert found.key.decode("utf-8") == "integration.probe"
        assert found.value["event_id"] == event_id
        assert found.value["event_type"] == "integration.probe"

        await producer.stop()
    finally:
        await consumer.stop()
