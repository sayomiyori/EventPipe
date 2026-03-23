from datetime import datetime, timezone

import grpc
import pytest
from google.protobuf import struct_pb2, timestamp_pb2

from ingest_service.app.generated import event_pb2, event_pb2_grpc
from ingest_service.app.grpc_server.servicer import EventServiceServicer
from ingest_service.tests.conftest import FakeKafkaProducer


@pytest.mark.asyncio
async def test_grpc_ingest() -> None:
    producer = FakeKafkaProducer()
    server = grpc.aio.server()
    event_pb2_grpc.add_EventServiceServicer_to_server(
        EventServiceServicer(producer),
        server,
    )
    bound_port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{bound_port}")
        stub = event_pb2_grpc.EventServiceStub(channel)
        ts = timestamp_pb2.Timestamp()
        ts.FromDatetime(datetime(2024, 1, 1, tzinfo=timezone.utc))
        st = struct_pb2.Struct()
        st.update({"k": "v"})
        req = event_pb2.Event(
            event_id="",
            source="grpc-test",
            event_type="order.created",
            timestamp=ts,
            payload=st,
            metadata={"trace": "1"},
        )
        resp = await stub.Ingest(req)
        assert resp.status == "accepted"
        assert resp.event_id
        assert len(producer.published) == 1
        et, body = producer.published[0]
        assert et == "order.created"
        assert body["event_type"] == "order.created"
        assert body["payload"] == {"k": "v"}
        await channel.close()
    finally:
        await server.stop(5)


@pytest.mark.asyncio
async def test_grpc_ingest_stream() -> None:
    producer = FakeKafkaProducer()
    server = grpc.aio.server()
    event_pb2_grpc.add_EventServiceServicer_to_server(
        EventServiceServicer(producer),
        server,
    )
    bound_port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{bound_port}")
        stub = event_pb2_grpc.EventServiceStub(channel)

        async def gen():
            for i in range(2):
                yield event_pb2.Event(
                    source="s",
                    event_type=f"type.{i}",
                    payload=struct_pb2.Struct(),
                )

        resp = await stub.IngestStream(gen())
        assert resp.status == "accepted"
        assert len(producer.published) == 2
        await channel.close()
    finally:
        await server.stop(5)
