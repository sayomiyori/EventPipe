import asyncio
import hashlib
import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import aioboto3
import httpx
import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
from aiokafka.structs import OffsetAndMetadata, TopicPartition
from pydantic import ValidationError
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
from transform_service.app.storage.postgres import (
    get_by_event_id,
    save_failed_event,
    save_processed_event,
)
from transform_service.app.storage.s3 import create_s3_session, upload_raw_json


def _parse_date_prefix(timestamp: str) -> str:
    try:
        raw = (timestamp or "").strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw).astimezone(UTC)
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
        except (ValidationError, ValueError, TypeError):
            raise
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            log.warning(
                "transform_retry",
                event_ref=hashlib.sha256(event_id.encode()).hexdigest(),
                attempt=attempt + 1,
                max_retries=max_retries,
                error=type(exc).__name__,
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
    body = {"error": error, "original": original}
    key_str = "invalid-event"
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
    raw_text = raw_bytes.decode("utf-8", errors="replace") if isinstance(raw_bytes, (bytes, bytearray)) else str(raw_bytes)
    kafka_key = msg.key.decode("utf-8", errors="replace") if msg.key else None
    record_sha256 = hashlib.sha256(bytes(raw_bytes) if isinstance(raw_bytes, (bytes, bytearray)) else raw_text.encode("utf-8")).hexdigest()

    async def dead_letter(error: str, tenant_id: UUID | None = None) -> None:
        diagnostic = {
            "topic": msg.topic, "partition": msg.partition, "offset": msg.offset,
            "tenant_id": tenant_id, "error": error, "record_sha256": record_sha256,
        }
        await publish_dlq(dlq_producer, settings, error=error, original={
            **diagnostic, "tenant_id": str(tenant_id) if tenant_id else None,
        }, kafka_key=kafka_key)
        if session_factory is not None:
            async with session_factory() as session:
                await save_failed_event(session, diagnostic)
    try:
        if isinstance(raw_bytes, (bytes, bytearray)):
            raw_text = raw_bytes.decode("utf-8")
        raw_dict = json.loads(raw_text)
        if not isinstance(raw_dict, dict):
            raise TypeError("event must be a JSON object")
    except (UnicodeDecodeError, ValueError, TypeError) as exc:
        events_failed_total.inc()
        log.error("invalid_json", error=type(exc).__name__, record_sha256=record_sha256)
        await dead_letter(type(exc).__name__)
        return

    event_id = str(raw_dict.get("event_id", "unknown"))
    tenant_id = None
    try:
        if raw_dict.get("tenant_id") is not None:
            tenant_id = UUID(str(raw_dict["tenant_id"]))
        if settings.platform_mode and tenant_id is None:
            raise ValueError("missing tenant context")
    except ValueError:
        await dead_letter("InvalidTenantContext")
        return

    async def _once() -> None:
        with transform_duration_seconds.time():
            normalized, enrichments = await run_pipeline(
                raw_dict,
                settings=settings,
                http_client=http_client,
            )
            async with session_factory() as session:
                existing = await get_by_event_id(session, event_id)
                if existing is not None:
                    if existing.tenant_id != tenant_id or (tenant_id and existing.raw_sha256 != record_sha256):
                        raise ValueError("event identity conflict")
                    if existing.raw_sha256 == record_sha256:
                        return
            ts = str(raw_dict.get("timestamp", ""))
            date_prefix = _parse_date_prefix(ts)
            eid = normalized["event_id"]
            s3_key = f"{date_prefix}/{record_sha256}/{eid}.json"
            if tenant_id:
                s3_key = f"tenants/{tenant_id}/{s3_key}"
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
                    tenant_id=tenant_id,
                    raw_sha256=record_sha256,
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
    except (ValidationError, ValueError, TypeError) as exc:
        events_failed_total.inc()
        log.error("transform_failed", record_sha256=record_sha256, error=type(exc).__name__)
        await dead_letter(type(exc).__name__, tenant_id)
    except Exception as exc:  # noqa: BLE001 — sanitize unknown infrastructure failures before exit
        # Infrastructure failures must replay from the uncommitted source offset.
        log.error("transform_unavailable", record_sha256=record_sha256, error=type(exc).__name__)
        raise RuntimeError("transform infrastructure unavailable") from None


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

    async def _start_with_retry(client, name: str, max_retries: int = 10, backoff_base: float = 2.0) -> None:
        for attempt in range(max_retries):
            try:
                await client.start()
                log.info("kafka_connected", client=name, attempt=attempt + 1)
                return
            except (KafkaError, OSError, TimeoutError) as exc:
                wait = backoff_base * (2 ** attempt)
                await client.stop()
                log.warning("kafka_not_ready", client=name, attempt=attempt + 1, max_retries=max_retries, error=type(exc).__name__, retry_in=wait)
                if attempt < max_retries - 1:
                    await asyncio.sleep(wait)
        raise RuntimeError(f"Could not connect {name} to Kafka after {max_retries} attempts")

    try:
        await _start_with_retry(consumer, "consumer")
        await _start_with_retry(dlq_producer, "dlq_producer")
        async with httpx.AsyncClient() as http_client:
            while not stop_event.is_set():
                result = await consumer.getmany(timeout_ms=1000, max_records=20)
                if stop_event.is_set():
                    break
                for _tp, messages in result.items():
                    for msg in messages:
                        lag = max(0, (consumer.highwater(_tp) or 0) - msg.offset - 1)
                        kafka_consumer_lag.set(lag)
                        await process_kafka_message(
                            msg,
                            settings=settings,
                            session_factory=session_factory,
                            s3_session=s3_session,
                            http_client=http_client,
                            dlq_producer=dlq_producer,
                            log=log,
                        )
                        # A failed DLQ delivery must leave the source event available for replay.
                        await commit_message(consumer, msg)
    finally:
        await dlq_producer.stop()
        await consumer.stop()
