import asyncio
import logging
import signal
import sys

import structlog
from prometheus_client import start_http_server

from transform_service.app.config import get_settings
from transform_service.app.consumer import run_consumer_loop
from transform_service.app.db.session import create_engine, create_session_factory, init_db
from transform_service.app.storage.s3 import create_s3_session, ensure_bucket_exists


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(colors=False),
        ],
        cache_logger_on_first_use=True,
    )


async def _async_main() -> None:
    log = structlog.get_logger("transform_main")
    configure_logging()
    settings = get_settings()
    start_http_server(port=settings.metrics_port, addr=settings.metrics_host)
    engine = create_engine(settings)
    await init_db(engine)
    session_factory = create_session_factory(engine)
    s3_session = create_s3_session()
    await ensure_bucket_exists(s3_session, settings)
    log.info(
        "transform_starting",
        kafka=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic_raw,
        group=settings.kafka_group_id,
    )
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _stop() -> None:
        stop_event.set()

    try:
        loop.add_signal_handler(signal.SIGINT, _stop)
        loop.add_signal_handler(signal.SIGTERM, _stop)
    except NotImplementedError:
        pass

    try:
        await run_consumer_loop(settings, session_factory, stop_event)
    finally:
        await engine.dispose()
        log.info("transform_stopped")


def main() -> None:
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
