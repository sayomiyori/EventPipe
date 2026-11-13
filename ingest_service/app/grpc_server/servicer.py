from datetime import UTC, datetime
from uuid import UUID

import grpc
from aiokafka.errors import KafkaError
from fastapi import HTTPException
from google.protobuf.json_format import MessageToDict
from pydantic import ValidationError

from eventpipe.platform import authorize_tenant
from ingest_service.app.config import Settings, get_settings
from ingest_service.app.generated import event_pb2, event_pb2_grpc
from ingest_service.app.kafka.producer import EventKafkaProducer
from ingest_service.app.schemas.event import EventIngest


def _timestamp_to_iso(event: event_pb2.Event) -> str:
    if not event.HasField("timestamp"):
        return datetime.now(UTC).isoformat()
    t = event.timestamp
    sec = t.seconds + t.nanos / 1e9
    return datetime.fromtimestamp(sec, tz=UTC).isoformat()


def _event_to_kafka_body(event: event_pb2.Event) -> dict:
    event_id = event.event_id or EventKafkaProducer.new_event_id()
    payload = MessageToDict(event.payload, preserving_proto_field_name=True)
    return {
        "event_id": event_id,
        "source": event.source,
        "event_type": event.event_type,
        "timestamp": _timestamp_to_iso(event),
        "payload": payload,
        "metadata": dict(event.metadata),
    }


class EventServiceServicer(event_pb2_grpc.EventServiceServicer):
    def __init__(self, producer: EventKafkaProducer, settings: Settings | None = None) -> None:
        self._producer = producer
        self._settings = settings or get_settings()

    async def _authorize(self, context) -> UUID | None:
        metadata = dict(context.invocation_metadata())
        try:
            return await authorize_tenant(
                platform_mode=self._settings.platform_mode, auth_url=self._settings.auth_url,
                authorization=metadata.get("authorization"),
                tenant_selector=metadata.get("x-tenant-id"), permission="tenant.manage",
            )
        except HTTPException as exc:
            codes = {401: grpc.StatusCode.UNAUTHENTICATED, 403: grpc.StatusCode.PERMISSION_DENIED,
                     404: grpc.StatusCode.NOT_FOUND, 422: grpc.StatusCode.INVALID_ARGUMENT,
                     503: grpc.StatusCode.UNAVAILABLE}
            await context.abort(codes[exc.status_code], exc.detail)

    async def _body(self, request, context, tenant_id):
        try:
            body = _event_to_kafka_body(request)
            EventIngest.model_validate({key: body[key] for key in ("source", "event_type", "payload", "metadata")})
            if len(body["event_id"]) > 64 or request.ByteSize() > 1024 * 1024:
                raise ValueError("event too large")
        except (ValidationError, ValueError, OverflowError):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "invalid event")
        if tenant_id:
            body["event_id"] = self._producer.new_event_id()
            body["tenant_id"] = str(tenant_id)
        return body

    async def _publish(self, request, body, context):
        try:
            await self._producer.publish_raw_event(event_type=request.event_type, body=body)
        except (KafkaError, OSError, TimeoutError):
            await context.abort(grpc.StatusCode.UNAVAILABLE, "event broker unavailable")

    async def Ingest(
        self,
        request: event_pb2.Event,
        context: grpc.aio.ServicerContext,
    ) -> event_pb2.IngestResponse:
        tenant_id = await self._authorize(context)
        if not request.event_type or not request.source:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "source and event_type are required",
            )
        body = await self._body(request, context, tenant_id)
        await self._publish(request, body, context)
        return event_pb2.IngestResponse(event_id=body["event_id"], status="accepted")

    async def IngestStream(
        self,
        request_iterator,
        context: grpc.aio.ServicerContext,
    ) -> event_pb2.IngestResponse:
        last_id = ""
        count = 0
        await self._authorize(context)
        async for request in request_iterator:
            if count >= 1000:
                await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "stream event limit exceeded")
            if not request.event_type or not request.source:
                await context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "source and event_type are required for each event",
                )
            tenant_id = await self._authorize(context)
            body = await self._body(request, context, tenant_id)
            await self._publish(request, body, context)
            last_id = body["event_id"]
            count += 1
        if count == 0:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "empty stream")
        return event_pb2.IngestResponse(event_id=last_id, status="accepted")
