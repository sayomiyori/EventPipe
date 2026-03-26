from datetime import UTC, datetime, timedelta
from typing import Any

import aioboto3
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition
from botocore.config import Config
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from query_service.app.config import Settings, get_settings
from query_service.app.db.session import ProcessedEvent
from query_service.app.metrics import query_duration_seconds

router = APIRouter(tags=["query-events"])


def _payload_preview(payload: dict[str, Any], limit: int = 200) -> str:
    text = str(payload)
    return text if len(text) <= limit else text[:limit] + "..."


def _s3_client_kwargs(settings: Settings) -> dict[str, Any]:
    return {
        "endpoint_url": settings.minio_endpoint_url,
        "aws_access_key_id": settings.minio_access_key,
        "aws_secret_access_key": settings.minio_secret_key,
        "region_name": settings.minio_region,
        "config": Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    }


def _get_session(request: Request) -> AsyncSession:
    session = getattr(request.state, "db_session", None)
    if session is None:
        raise RuntimeError("DB session missing from request state")
    return session


SessionDep = Depends(_get_session)
SettingsDep = Depends(get_settings)


@router.get("/events")
async def list_events(
    source: str | None = None,
    event_type: str | None = None,
    from_dt: datetime | None = Query(None, alias="from"),
    to_dt: datetime | None = Query(None, alias="to"),
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=200),
    session: AsyncSession = SessionDep,
) -> list[dict[str, Any]]:
    with query_duration_seconds.time():
        stmt = select(ProcessedEvent).order_by(desc(ProcessedEvent.processed_at))
        if source:
            stmt = stmt.where(ProcessedEvent.source == source)
        if event_type:
            stmt = stmt.where(ProcessedEvent.event_type == event_type)
        if from_dt:
            stmt = stmt.where(ProcessedEvent.processed_at >= from_dt)
        if to_dt:
            stmt = stmt.where(ProcessedEvent.processed_at <= to_dt)
        stmt = stmt.offset((page - 1) * size).limit(size)
        rows = (await session.execute(stmt)).scalars().all()
    return [
        {
            "event_id": r.event_id,
            "source": r.source,
            "event_type": r.event_type,
            "processed_at": r.processed_at.isoformat(),
            "payload_preview": _payload_preview(r.payload),
        }
        for r in rows
    ]


@router.get("/events/{event_id}")
async def get_event(event_id: str, session: AsyncSession = SessionDep) -> dict[str, Any]:
    with query_duration_seconds.time():
        row = (
            await session.execute(select(ProcessedEvent).where(ProcessedEvent.event_id == event_id))
        ).scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="event not found")
    return {
        "event_id": row.event_id,
        "source": row.source,
        "event_type": row.event_type,
        "processed_at": row.processed_at.isoformat(),
        "payload": row.payload,
        "metadata": row.metadata_,
        "enrichments": row.enrichments,
        "s3_key": row.s3_key,
    }


@router.get("/events/{event_id}/raw")
async def get_event_raw(event_id: str, session: AsyncSession = SessionDep, settings: Settings = SettingsDep):
    with query_duration_seconds.time():
        row = (
            await session.execute(select(ProcessedEvent).where(ProcessedEvent.event_id == event_id))
        ).scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="event not found")

    async with aioboto3.Session().client("s3", **_s3_client_kwargs(settings)) as client:
        presigned = await client.generate_presigned_url(
            "get_object",
            Params={"Bucket": settings.minio_bucket_raw, "Key": row.s3_key},
            ExpiresIn=15 * 60,
        )
    # Replace internal Docker hostname with public URL accessible from the browser
    presigned = presigned.replace(settings.minio_endpoint_url, settings.minio_public_url, 1)
    return RedirectResponse(url=presigned, status_code=307)


async def _count_dlq(settings: Settings) -> int:
    consumer = AIOKafkaConsumer(
        settings.kafka_topic_dlq,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        enable_auto_commit=False,
        group_id=None,
    )
    await consumer.start()
    try:
        partitions = consumer.partitions_for_topic(settings.kafka_topic_dlq) or set()
        if not partitions:
            return 0
        tps = [TopicPartition(settings.kafka_topic_dlq, p) for p in partitions]
        ends = await consumer.end_offsets(tps)
        return sum(v for v in ends.values())
    finally:
        await consumer.stop()


@router.get("/stats")
async def get_stats(session: AsyncSession = SessionDep, settings: Settings = SettingsDep) -> dict[str, Any]:
    with query_duration_seconds.time():
        total = await session.scalar(select(func.count()).select_from(ProcessedEvent))
        from_24h = datetime.now(UTC) - timedelta(hours=24)
        by_source_rows = (
            await session.execute(
                select(ProcessedEvent.source, func.count())
                .where(ProcessedEvent.processed_at >= from_24h)
                .group_by(ProcessedEvent.source)
            )
        ).all()
        by_type_rows = (
            await session.execute(
                select(ProcessedEvent.event_type, func.count())
                .where(ProcessedEvent.processed_at >= from_24h)
                .group_by(ProcessedEvent.event_type)
            )
        ).all()
        dlq_count = await _count_dlq(settings)
    return {
        "events_total": int(total or 0),
        "events_by_source": {k: int(v) for k, v in by_source_rows},
        "events_by_type": {k: int(v) for k, v in by_type_rows},
        "dlq_count": int(dlq_count),
    }
