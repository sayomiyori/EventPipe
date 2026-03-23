from datetime import datetime, timezone

import grpc
from google.protobuf.json_format import MessageToDict

from ingest_service.app.generated import event_pb2, event_pb2_grpc
from ingest_service.app.kafka.producer import EventKafkaProducer


def _timestamp_to_iso(event: event_pb2.Event) -> str:
    if not event.HasField("timestamp"):
        return datetime.now(timezone.utc).isoformat()
    t = event.timestamp
    sec = t.seconds + t.nanos / 1e9
    return datetime.fromtimestamp(sec, tz=timezone.utc).isoformat()


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
    def __init__(self, producer: EventKafkaProducer) -> None:
        self._producer = producer

    async def Ingest(
        self,
        request: event_pb2.Event,
        context: grpc.aio.ServicerContext,
    ) -> event_pb2.IngestResponse:
        if not request.event_type or not request.source:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "source and event_type are required",
            )
        body = _event_to_kafka_body(request)
        await self._producer.publish_raw_event(
            event_type=request.event_type,
            body=body,
        )
        return event_pb2.IngestResponse(event_id=body["event_id"], status="accepted")

    async def IngestStream(
        self,
        request_iterator,
        context: grpc.aio.ServicerContext,
    ) -> event_pb2.IngestResponse:
        last_id = ""
        count = 0
        async for request in request_iterator:
            if not request.event_type or not request.source:
                await context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "source and event_type are required for each event",
                )
            body = _event_to_kafka_body(request)
            await self._producer.publish_raw_event(
                event_type=request.event_type,
                body=body,
            )
            last_id = body["event_id"]
            count += 1
        if count == 0:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "empty stream")
        return event_pb2.IngestResponse(event_id=last_id, status="accepted")
