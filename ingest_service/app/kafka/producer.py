import asyncio
import json
import logging
from typing import Any
from uuid import uuid4

from aiokafka import AIOKafkaProducer

from ingest_service.app.config import Settings

log = logging.getLogger(__name__)


def _json_serialize(value: Any) -> bytes:
    return json.dumps(value, default=str).encode("utf-8")


class EventKafkaProducer:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer: AIOKafkaProducer | None = None

    async def start(self, *, max_retries: int = 10, backoff_base: float = 2.0) -> None:
        for attempt in range(max_retries):
            try:
                self._producer = AIOKafkaProducer(
                    bootstrap_servers=self._settings.kafka_bootstrap_servers,
                    value_serializer=_json_serialize,
                    key_serializer=lambda k: k if isinstance(k, bytes) else str(k).encode("utf-8"),
                )
                await self._producer.start()
                log.info("Kafka producer connected on attempt %d", attempt + 1)
                return
            except Exception as exc:
                wait = backoff_base * (2 ** attempt)
                log.warning("Kafka not ready (attempt %d/%d): %s — retrying in %.0fs", attempt + 1, max_retries, exc, wait)
                self._producer = None
                if attempt < max_retries - 1:
                    await asyncio.sleep(wait)
        raise RuntimeError(f"Could not connect to Kafka after {max_retries} attempts")

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
