from grpc import aio

from ingest_service.app.config import Settings
from ingest_service.app.generated import event_pb2_grpc
from ingest_service.app.grpc_server.servicer import EventServiceServicer
from ingest_service.app.kafka.producer import EventKafkaProducer


async def serve_grpc(settings: Settings, producer: EventKafkaProducer) -> aio.Server:
    server = aio.server()
    event_pb2_grpc.add_EventServiceServicer_to_server(
        EventServiceServicer(producer),
        server,
    )
    addr = f"{settings.grpc_host}:{settings.grpc_port}"
    server.add_insecure_port(addr)
    await server.start()
    return server
