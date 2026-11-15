from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from transform_service.app.models.processed_event import FailedEvent, ProcessedEvent


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
    tenant_id: UUID | None = None,
    raw_sha256: str | None = None,
) -> ProcessedEvent:
    ts = processed_at or datetime.now(UTC)
    stmt = insert(ProcessedEvent).values(
        event_id=event_id, source=source, event_type=event_type, payload=payload,
        metadata=metadata, s3_key=s3_key, enrichments=enrichments, processed_at=ts,
        tenant_id=tenant_id, raw_sha256=raw_sha256,
    )
    if tenant_id is None:
        # Standalone event IDs historically overwrite legacy records only.
        stmt = stmt.on_conflict_do_update(
            index_elements=["event_id"], where=ProcessedEvent.tenant_id.is_(None),
            set_={key: getattr(stmt.excluded, key) for key in (
                "source", "event_type", "payload", "metadata", "s3_key", "enrichments",
                "processed_at", "raw_sha256",
            )},
        )
    else:
        stmt = stmt.on_conflict_do_nothing(index_elements=["event_id"])
    await session.execute(stmt)
    row = await get_by_event_id(session, event_id)
    if row.tenant_id != tenant_id or row.raw_sha256 != raw_sha256:
        raise ValueError("event identity conflict")
    await session.commit()
    return row


async def get_by_event_id(session: AsyncSession, event_id: str) -> ProcessedEvent | None:
    q = await session.execute(select(ProcessedEvent).where(ProcessedEvent.event_id == event_id))
    return q.scalar_one_or_none()


async def save_failed_event(session: AsyncSession, diagnostic: dict) -> None:
    await session.execute(insert(FailedEvent).values(**diagnostic).on_conflict_do_nothing())
    await session.commit()
