import pytest

from ingest_service.app.config import Settings
from ingest_service.app.kafka.producer import EventKafkaProducer


@pytest.mark.asyncio
async def test_publish_raw_event_uses_topic_and_key(mock_aiokafka_producer) -> None:
    from unittest.mock import patch

    settings = Settings(
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic_raw="events.raw",
    )
    prod = EventKafkaProducer(settings)

    with patch(
        "ingest_service.app.kafka.producer.AIOKafkaProducer",
        return_value=mock_aiokafka_producer,
    ):
        await prod.start()
        await prod.publish_raw_event(
            event_type="my.type",
            body={"event_id": "1", "payload": {}},
        )
        await prod.stop()

    mock_aiokafka_producer.start.assert_awaited_once()
    mock_aiokafka_producer.stop.assert_awaited_once()
    mock_aiokafka_producer.send_and_wait.assert_awaited_once()
    args, kwargs = mock_aiokafka_producer.send_and_wait.await_args
    topic = kwargs.get("topic", args[0] if args else None)
    key = kwargs.get("key", args[1] if len(args) > 1 else None)
    value = kwargs.get("value", args[2] if len(args) > 2 else None)
    assert topic == "events.raw"
    assert key == "my.type"
    assert value["event_id"] == "1"


@pytest.mark.asyncio
async def test_publish_without_start_raises() -> None:
    settings = Settings()
    prod = EventKafkaProducer(settings)
    with pytest.raises(RuntimeError, match="not started"):
        await prod.publish_raw_event(event_type="t", body={})
