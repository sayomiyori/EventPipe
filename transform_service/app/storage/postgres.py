from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from transform_service.app.models.processed_event import ProcessedEvent


async def save_processed_event(
    session: AsyncSession,
    *,
    event_id: str,
    source: str,
    event_type: str,
    payload: dict[str, Any],
    metadata: dict[str, Any],
    s3_key: str,
    enrichments: dict[str, Any],
    processed_at: datetime | None = None,
) -> ProcessedEvent:
    ts = processed_at or datetime.now(timezone.utc)
    result = await session.execute(select(ProcessedEvent).where(ProcessedEvent.event_id == event_id))
    existing = result.scalar_one_or_none()
    if existing:
        existing.source = source
        existing.event_type = event_type
        existing.payload = payload
        existing.metadata_ = metadata
        existing.s3_key = s3_key
        existing.enrichments = enrichments
        existing.processed_at = ts
        await session.commit()
        await session.refresh(existing)
        return existing

    row = ProcessedEvent(
        event_id=event_id,
        source=source,
        event_type=event_type,
        payload=payload,
        metadata_=metadata,
        s3_key=s3_key,
        enrichments=enrichments,
        processed_at=ts,
    )
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return row


async def get_by_event_id(session: AsyncSession, event_id: str) -> ProcessedEvent | None:
    q = await session.execute(select(ProcessedEvent).where(ProcessedEvent.event_id == event_id))
    return q.scalar_one_or_none()
