import grpc
import grpc_testing
from google.protobuf import struct_pb2

from ingest_service.app.generated import event_pb2, event_pb2_grpc


class _StubEventService(event_pb2_grpc.EventServiceServicer):
    def Ingest(self, request: event_pb2.Event, context) -> event_pb2.IngestResponse:
        return event_pb2.IngestResponse(event_id="stub-id", status="accepted")

    def IngestStream(self, request_iterator, context) -> event_pb2.IngestResponse:
        count = 0
        for _ in request_iterator:
            count += 1
        if count == 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("empty stream")
            return event_pb2.IngestResponse()
        return event_pb2.IngestResponse(event_id="batch", status="accepted")


def test_grpcio_testing_unary_ingest_descriptor() -> None:
    service = event_pb2.DESCRIPTOR.services_by_name["EventService"]
    method = service.methods_by_name["Ingest"]
    server = grpc_testing.server_from_dictionary(
        {service: _StubEventService()},
        grpc_testing.strict_real_time(),
    )
    st = struct_pb2.Struct()
    req = event_pb2.Event(source="s", event_type="t", payload=st)
    rpc = server.invoke_unary_unary(method, (), req, None)
    response, _trailing, code, _details = rpc.termination()
    assert code == grpc.StatusCode.OK
    assert response.status == "accepted"
    assert response.event_id == "stub-id"


def test_grpcio_testing_stream_ingest_descriptor() -> None:
    service = event_pb2.DESCRIPTOR.services_by_name["EventService"]
    method = service.methods_by_name["IngestStream"]
    server = grpc_testing.server_from_dictionary(
        {service: _StubEventService()},
        grpc_testing.strict_real_time(),
    )
    rpc = server.invoke_stream_unary(method, (), None)
    st = struct_pb2.Struct()
    rpc.send_request(event_pb2.Event(source="s", event_type="t1", payload=st))
    rpc.send_request(event_pb2.Event(source="s", event_type="t2", payload=st))
    rpc.requests_closed()
    response, _trailing, code, _details = rpc.termination()
    assert code == grpc.StatusCode.OK
    assert response.status == "accepted"
