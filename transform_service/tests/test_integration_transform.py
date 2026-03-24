import json
import os
import uuid

import pytest


@pytest.mark.integration
@pytest.mark.asyncio
async def test_kafka_to_minio_and_postgres(integration_env) -> None:
    import structlog
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
    from botocore.config import Config
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from transform_service.app.config import Settings
    from transform_service.app.consumer import process_kafka_message
    from transform_service.app.db.session import create_engine, create_session_factory, init_db
    from transform_service.app.storage.postgres import get_by_event_id
    from transform_service.app.storage.s3 import create_s3_session, ensure_bucket_exists

    def s3_kwargs(settings: Settings) -> dict:
        return {
            "endpoint_url": settings.minio_endpoint_url,
            "aws_access_key_id": settings.minio_access_key,
            "aws_secret_access_key": settings.minio_secret_key,
            "region_name": settings.minio_region,
            "config": Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        }

    settings = Settings(
        kafka_bootstrap_servers=os.environ.get("TRANSFORM_KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        kafka_topic_raw="events.raw",
        kafka_topic_dlq="events.dlq",
        kafka_group_id="transform-workers",
        database_url=os.environ.get(
            "TRANSFORM_DATABASE_URL",
            "postgresql+asyncpg://eventpipe:eventpipe@localhost:5432/eventpipe",
        ),
        minio_endpoint_url=os.environ.get("TRANSFORM_MINIO_ENDPOINT_URL", "http://localhost:9000"),
        minio_access_key=os.environ.get("TRANSFORM_MINIO_ACCESS_KEY", "minio"),
        minio_secret_key=os.environ.get("TRANSFORM_MINIO_SECRET_KEY", "minio12345"),
        minio_bucket_raw="raw-events",
        max_retries=3,
    )

    event_id = str(uuid.uuid4())
    body = {
        "event_id": event_id,
        "source": "integration",
        "event_type": "integration.test",
        "timestamp": "2025-01-10T15:00:00+00:00",
        "payload": {"hello": "world"},
        "metadata": {"run": "1"},
    }
    payload = json.dumps(body).encode("utf-8")

    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"it-transform-{uuid.uuid4().hex[:12]}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: b,
    )
    await consumer.start()
    consumer.subscribe([settings.kafka_topic_raw])
    for _ in range(100):
        assignment = consumer.assignment()
        if assignment:
            break
        await consumer.getmany(timeout_ms=500)
    assert consumer.assignment(), "Kafka consumer did not get partition assignment"

    for tp in consumer.assignment():
        await consumer.seek_to_end(tp)

    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: v if isinstance(v, bytes) else str(v).encode("utf-8"),
        key_serializer=lambda k: k if isinstance(k, bytes) else str(k).encode("utf-8"),
    )
    await producer.start()
    await producer.send_and_wait(settings.kafka_topic_raw, key=body["event_type"], value=payload)
    await producer.stop()

    msg = None
    for _ in range(60):
        batch = await consumer.getmany(timeout_ms=2000, max_records=20)
        for _tp, messages in batch.items():
            for m in messages:
                try:
                    data = json.loads(m.value.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if data.get("event_id") == event_id:
                    msg = m
                    break
            if msg:
                break
        if msg:
            break
    await consumer.stop()
    assert msg is not None

    engine = create_engine(settings)
    await init_db(engine)
    session_factory: async_sessionmaker[AsyncSession] = create_session_factory(engine)
    s3_session = create_s3_session()
    await ensure_bucket_exists(s3_session, settings)

    dlq_producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k if isinstance(k, bytes) else str(k).encode("utf-8"),
    )
    await dlq_producer.start()
    log = structlog.get_logger("integration_test")
    try:
        await process_kafka_message(
            msg,
            settings=settings,
            session_factory=session_factory,
            s3_session=s3_session,
            http_client=None,
            dlq_producer=dlq_producer,
            log=log,
        )
    finally:
        await dlq_producer.stop()

    async with session_factory() as session:
        row = await get_by_event_id(session, event_id)

    await engine.dispose()

    assert row is not None
    assert row.event_type == "integration.test"
    assert row.s3_key.startswith("2025-01-10/")
    assert row.s3_key.endswith(f"{event_id}.json")

    async with s3_session.client("s3", **s3_kwargs(settings)) as client:
        obj = await client.get_object(Bucket=settings.minio_bucket_raw, Key=row.s3_key)
        raw_read = await obj["Body"].read()
    assert json.loads(raw_read.decode())["event_id"] == event_id
