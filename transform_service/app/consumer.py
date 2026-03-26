import asyncio
import json
from datetime import datetime, timezone
from typing import Any

import aioboto3
import httpx
import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import OffsetAndMetadata, TopicPartition
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from transform_service.app.config import Settings
from transform_service.app.metrics import (
    events_failed_total,
    events_processed_total,
    kafka_consumer_lag,
    s3_upload_duration_seconds,
    transform_duration_seconds,
)
from transform_service.app.pipeline.pipeline import run_pipeline
from transform_service.app.storage.postgres import save_processed_event
from transform_service.app.storage.s3 import create_s3_session, upload_raw_json


def _parse_date_prefix(timestamp: str) -> str:
    try:
        raw = (timestamp or "").strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw).astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return "unknown-date"


async def _with_retries(
    factory,
    *,
    max_retries: int,
    backoff_base: float,
    log: structlog.stdlib.BoundLogger,
    event_id: str,
) -> None:
    last_exc: BaseException | None = None
    for attempt in range(max_retries):
        try:
            await factory()
            return
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            log.warning(
                "transform_retry",
                event_id=event_id,
                attempt=attempt + 1,
                max_retries=max_retries,
                error=str(exc),
            )
            if attempt < max_retries - 1:
                await asyncio.sleep(backoff_base * (2**attempt))
    assert last_exc is not None
    raise last_exc


async def publish_dlq(
    producer: AIOKafkaProducer,
    settings: Settings,
    *,
    error: str,
    original: dict[str, Any],
    kafka_key: str | None,
) -> None:
    body = {
        "error": error,
        "original": original,
    }
    key_str = kafka_key or str(original.get("event_type") or "unknown")
    await producer.send_and_wait(
        settings.kafka_topic_dlq,
        key=key_str,
        value=body,
    )


async def process_kafka_message(
    msg,
    *,
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    s3_session: aioboto3.Session,
    http_client: httpx.AsyncClient | None,
    dlq_producer: AIOKafkaProducer,
    log: structlog.stdlib.BoundLogger,
) -> None:
    raw_bytes = msg.value
    raw_text = raw_bytes.decode("utf-8") if isinstance(raw_bytes, (bytes, bytearray)) else str(raw_bytes)
    kafka_key = msg.key.decode("utf-8") if msg.key else None
    try:
        raw_dict = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        log.error("invalid_json", error=str(exc))
        await publish_dlq(
            dlq_producer,
            settings,
            error=f"json_decode:{exc}",
            original={"raw": raw_text[:8000]},
            kafka_key=kafka_key,
        )
        return

    event_id = str(raw_dict.get("event_id", "unknown"))

    async def _once() -> None:
        with transform_duration_seconds.time():
            normalized, enrichments = await run_pipeline(
                raw_dict,
                settings=settings,
                http_client=http_client,
            )
            ts = str(raw_dict.get("timestamp", ""))
            date_prefix = _parse_date_prefix(ts)
            eid = normalized["event_id"]
            s3_key = f"{date_prefix}/{eid}.json"
            with s3_upload_duration_seconds.time():
                await upload_raw_json(
                    s3_session,
                    settings,
                    key=s3_key,
                    body=raw_text.encode("utf-8"),
                )
            async with session_factory() as session:
                await save_processed_event(
                    session,
                    event_id=eid,
                    source=normalized["source"],
                    event_type=normalized["event_type"],
                    payload=normalized.get("payload") or {},
                    metadata=normalized.get("metadata") or {},
                    s3_key=s3_key,
                    enrichments=enrichments,
                )

    try:
        await _with_retries(
            _once,
            max_retries=settings.max_retries,
            backoff_base=settings.retry_backoff_base_seconds,
            log=log,
            event_id=event_id,
        )
        events_processed_total.inc()
    except Exception as exc:  # noqa: BLE001
        events_failed_total.inc()
        log.exception("transform_failed", event_id=event_id, error=str(exc))
        await publish_dlq(
            dlq_producer,
            settings,
            error=str(exc),
            original=raw_dict,
            kafka_key=kafka_key,
        )


async def commit_message(consumer: AIOKafkaConsumer, msg) -> None:
    tp = TopicPartition(msg.topic, msg.partition)
    await consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, "")})


async def run_consumer_loop(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    stop_event: asyncio.Event,
) -> None:
    log = structlog.get_logger("transform_consumer")
    consumer = AIOKafkaConsumer(
        settings.kafka_topic_raw,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: b,
    )
    dlq_producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k if isinstance(k, bytes) else str(k).encode("utf-8"),
    )
    s3_session = create_s3_session()
    await consumer.start()
    await dlq_producer.start()
    try:
        async with httpx.AsyncClient() as http_client:
            while not stop_event.is_set():
                result = await consumer.getmany(timeout_ms=1000, max_records=20)
                if stop_event.is_set():
                    break
                for _tp, messages in result.items():
                    for msg in messages:
                        lag = max(0, (consumer.highwater(_tp) or 0) - msg.offset - 1)
                        kafka_consumer_lag.set(lag)
                        try:
                            await process_kafka_message(
                                msg,
                                settings=settings,
                                session_factory=session_factory,
                                s3_session=s3_session,
                                http_client=http_client,
                                dlq_producer=dlq_producer,
                                log=log,
                            )
                        finally:
                            await commit_message(consumer, msg)
    finally:
        await dlq_producer.stop()
        await consumer.stop()
