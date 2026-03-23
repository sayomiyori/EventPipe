import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from ingest_service.app.api.events import router
from ingest_service.app.config import get_settings
from ingest_service.app.grpc_server.server import serve_grpc
from ingest_service.app.kafka.producer import EventKafkaProducer


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    started_producer = False
    grpc_server = None
    grpc_task: asyncio.Task | None = None
    if getattr(app.state, "producer", None) is None:
        settings = get_settings()
        producer = EventKafkaProducer(settings)
        await producer.start()
        app.state.producer = producer
        started_producer = True
        grpc_server = await serve_grpc(settings, producer)
        grpc_task = asyncio.create_task(grpc_server.wait_for_termination())
        app.state._ingest_grpc_server = grpc_server
        app.state._ingest_grpc_task = grpc_task
    try:
        yield
    finally:
        if grpc_server is not None and grpc_task is not None:
            await grpc_server.stop(5)
            grpc_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await grpc_task
        if started_producer:
            await app.state.producer.stop()


def create_app() -> FastAPI:
    application = FastAPI(
        title="EventPipe Ingest",
        lifespan=lifespan,
    )
    application.include_router(router, prefix="/api/v1")
    return application


app = create_app()


async def _run_dual() -> None:
    settings = get_settings()
    producer = EventKafkaProducer(settings)
    await producer.start()
    app.state.producer = producer
    grpc_server = await serve_grpc(settings, producer)
    uv_config = uvicorn.Config(
        app,
        host=settings.api_host,
        port=settings.api_port,
        log_level="info",
    )
    uv_server = uvicorn.Server(uv_config)
    try:
        await asyncio.gather(
            uv_server.serve(),
            grpc_server.wait_for_termination(),
        )
    finally:
        await grpc_server.stop(5)
        await producer.stop()


def main() -> None:
    asyncio.run(_run_dual())


if __name__ == "__main__":
    main()
