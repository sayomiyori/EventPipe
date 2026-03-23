import json
from typing import Any
from uuid import uuid4

from aiokafka import AIOKafkaProducer

from ingest_service.app.config import Settings


def _json_serialize(value: Any) -> bytes:
    return json.dumps(value, default=str).encode("utf-8")


class EventKafkaProducer:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            value_serializer=_json_serialize,
            key_serializer=lambda k: k if isinstance(k, bytes) else str(k).encode("utf-8"),
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()
            self._producer = None

    async def publish_raw_event(
        self,
        *,
        event_type: str,
        body: dict[str, Any],
    ) -> None:
        if not self._producer:
            raise RuntimeError("Kafka producer is not started")
        await self._producer.send_and_wait(
            self._settings.kafka_topic_raw,
            key=event_type,
            value=body,
        )

    @staticmethod
    def new_event_id() -> str:
        return str(uuid4())
