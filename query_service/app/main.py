from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware

from query_service.app.api.events import router
from query_service.app.config import get_settings
from query_service.app.db.session import create_engine, create_session_factory
from query_service.app.metrics import active_queries, metrics_response


class QueryMetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        active_queries.inc()
        try:
            return await call_next(request)
        finally:
            active_queries.dec()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    app.state.query_engine = engine
    app.state.query_session_factory = session_factory
    try:
        yield
    finally:
        await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="EventPipe Query Service", lifespan=lifespan)
    app.add_middleware(QueryMetricsMiddleware)

    @app.middleware("http")
    async def inject_session(request: Request, call_next):
        sf = request.app.state.query_session_factory
        async with sf() as session:
            request.state.db_session = session
            response = await call_next(request)
            return response

    app.include_router(router, prefix="/api/v1")
    app.add_api_route("/metrics", lambda: metrics_response(), methods=["GET"])
    return app


app = create_app()


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        "query_service.app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
