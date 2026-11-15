from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Request

from eventpipe.platform import authorize_tenant
from ingest_service.app.config import get_settings
from ingest_service.app.kafka.producer import EventKafkaProducer
from ingest_service.app.metrics import events_ingested_total, ingest_duration_seconds
from ingest_service.app.schemas.event import (
    BatchEventIngest,
    BatchEventIngestResponse,
    EventIngest,
    EventIngestResponse,
)

router = APIRouter(tags=["events"])


def get_producer(request: Request) -> EventKafkaProducer:
    p = getattr(request.app.state, "producer", None)
    if p is None:
        raise RuntimeError("Kafka producer is not initialized")
    return p


ProducerDep = Annotated[EventKafkaProducer, Depends(get_producer)]


async def authorize_ingest(request: Request) -> UUID | None:
    settings = get_settings()
    return await authorize_tenant(
        platform_mode=settings.platform_mode, auth_url=settings.auth_url,
        authorization=request.headers.get("authorization"),
        tenant_selector=request.headers.get("x-tenant-id"), permission="tenant.manage",
    )


TenantDep = Annotated[UUID | None, Depends(authorize_ingest)]


def _kafka_body(
    *,
    event_id: str,
    source: str,
    event_type: str,
    payload: dict,
    metadata: dict[str, str],
    ts: datetime | None = None,
    tenant_id: UUID | None = None,
) -> dict:
    t = ts or datetime.now(UTC)
    return {
        "event_id": event_id,
        "source": source,
        "event_type": event_type,
        "timestamp": t.isoformat(),
        "payload": payload,
        "metadata": metadata,
        **({"tenant_id": str(tenant_id)} if tenant_id else {}),
    }


@router.post("/events", response_model=EventIngestResponse)
async def ingest_event(body: EventIngest, producer: ProducerDep, tenant_id: TenantDep) -> EventIngestResponse:
    with ingest_duration_seconds.time():
        event_id = producer.new_event_id()
        await producer.publish_raw_event(
            event_type=body.event_type,
            body=_kafka_body(
                event_id=event_id,
                source=body.source,
                event_type=body.event_type,
                payload=body.payload,
                metadata=body.metadata,
                tenant_id=tenant_id,
            ),
        )
    events_ingested_total.inc()
    return EventIngestResponse(event_id=UUID(event_id))


@router.post("/events/batch", response_model=BatchEventIngestResponse)
async def ingest_events_batch(
    body: BatchEventIngest,
    producer: ProducerDep,
    tenant_id: TenantDep,
) -> BatchEventIngestResponse:
    ids: list[UUID] = []
    with ingest_duration_seconds.time():
        for ev in body.events:
            event_id = producer.new_event_id()
            await producer.publish_raw_event(
                event_type=ev.event_type,
                body=_kafka_body(
                    event_id=event_id,
                    source=ev.source,
                    event_type=ev.event_type,
                    payload=ev.payload,
                    metadata=ev.metadata,
                    tenant_id=tenant_id,
                ),
            )
            events_ingested_total.inc()
            ids.append(UUID(event_id))
    return BatchEventIngestResponse(count=len(ids), event_ids=ids)
