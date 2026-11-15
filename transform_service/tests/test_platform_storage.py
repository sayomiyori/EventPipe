import asyncio
import os
import uuid
from datetime import UTC, datetime

import httpx
import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import inspect, select, text
from sqlalchemy.engine import make_url

from ingest_service.tests.test_platform import mock_identity
from query_service.app.config import Settings as QuerySettings
from query_service.app.config import get_settings as query_settings
from query_service.app.main import create_app
from scripts.adopt_legacy_schema import adopt
from transform_service.app.config import Settings
from transform_service.app.db.session import create_engine, create_session_factory, init_db
from transform_service.app.models.processed_event import Base, ProcessedEvent
from transform_service.app.storage.postgres import save_failed_event, save_processed_event
from transform_service.app.storage.s3 import (
    create_s3_session,
    ensure_bucket_exists,
    upload_raw_json,
)


def migrate(connection, revision="head", operation="upgrade"):
    config = Config("alembic.ini")
    config.attributes["connection"] = connection
    getattr(command, operation)(config, revision)


async def isolated_database():
    url = make_url(os.environ["TRANSFORM_DATABASE_URL"])
    admin = create_engine(Settings(database_url=url.render_as_string(hide_password=False)))
    name = "closure_" + uuid.uuid4().hex
    try:
        async with admin.connect() as connection:
            connection = await connection.execution_options(isolation_level="AUTOCOMMIT")
            await connection.execute(text(f'CREATE DATABASE "{name}"'))
    finally:
        await admin.dispose()
    return create_engine(Settings(database_url=url.set(database=name).render_as_string(hide_password=False)))


@pytest.mark.integration
async def test_migration_roundtrip_models_and_checked_legacy_adoption(integration_env):
    engine = await isolated_database()
    legacy_id = str(uuid.uuid4())
    try:
        async with engine.begin() as connection:
            await connection.run_sync(migrate, "001_legacy_baseline")
            await connection.execute(text("""INSERT INTO processed_events
                (id,event_id,source,event_type,payload,metadata,s3_key,enrichments)
                VALUES (:id,:event_id,'legacy','legacy','{}','{}','legacy.json','{}')"""),
                {"id": uuid.uuid4(), "event_id": legacy_id})
            # Test-owned version table removed to reproduce the previous unversioned schema.
            await connection.execute(text("DROP TABLE alembic_version"))
            await connection.run_sync(adopt)
            await connection.run_sync(migrate)
            assert await connection.scalar(text("SELECT tenant_id FROM processed_events")) is None
            assert await connection.scalar(text("SELECT count(*) FROM processed_events")) == 1
            def drift(sync):
                return compare_metadata(MigrationContext.configure(sync, opts={"compare_type": True}), Base.metadata)
            assert await connection.run_sync(drift) == []
            await connection.run_sync(migrate, "001_legacy_baseline", "downgrade")
            assert "tenant_id" not in await connection.run_sync(lambda sync: {c["name"] for c in inspect(sync).get_columns("processed_events")})
            await connection.run_sync(migrate)
            assert await connection.scalar(text("SELECT count(*) FROM processed_events")) == 1
        await init_db(engine)
        async with engine.begin() as connection:
            await connection.run_sync(migrate, "base", "downgrade")
            assert await connection.run_sync(lambda sync: inspect(sync).get_table_names()) == ["alembic_version"]
            await connection.run_sync(migrate)
        await init_db(engine)
    finally:
        await engine.dispose()


@pytest.mark.integration
async def test_unversioned_unexpected_schema_is_rejected(integration_env):
    engine = await isolated_database()
    try:
        async with engine.begin() as connection:
            await connection.run_sync(migrate, "001_legacy_baseline")
            await connection.execute(text("DROP TABLE alembic_version"))
            await connection.execute(text("CREATE TABLE unexpected (id integer)"))
        with pytest.raises(RuntimeError, match="unexpected"):
            async with engine.begin() as connection:
                await connection.run_sync(adopt)
        with pytest.raises(RuntimeError, match="requires checked"):
            async with engine.begin() as connection:
                await connection.run_sync(migrate)
        async with engine.connect() as connection:
            assert await connection.run_sync(lambda sync: set(inspect(sync).get_table_names())) == {"processed_events", "unexpected"}
    finally:
        await engine.dispose()


@pytest.mark.integration
async def test_platform_query_scopes_all_reads_stats_legacy_and_concurrent_identity(integration_env, monkeypatch):
    engine = await isolated_database()
    tenants = [uuid.uuid4(), uuid.uuid4()]
    ids = [str(uuid.uuid4()) for _ in range(3)]
    try:
        async with engine.begin() as connection:
            await connection.run_sync(migrate)
        factory = create_session_factory(engine)
        storage_settings = Settings()
        s3_key = f"tenants/{tenants[0]}/{uuid.uuid4()}.json"
        s3 = create_s3_session()
        await ensure_bucket_exists(s3, storage_settings)
        await upload_raw_json(s3, storage_settings, key=s3_key, body=b'{"raw_fixture":true}')

        async def save(event_id, tenant_id, payload=None):
            async with factory() as session:
                return await save_processed_event(session, event_id=event_id, tenant_id=tenant_id,
                    source=str(tenant_id or "legacy"), event_type="fixture", payload=payload or {"fixture": True},
                    metadata={}, enrichments={}, s3_key=s3_key, raw_sha256="a" * 64)

        await asyncio.gather(*(save(ids[0], tenants[0]) for _ in range(5)))
        await save(ids[1], tenants[1])
        await save(ids[2], None)
        with pytest.raises(ValueError, match="conflict"):
            await save(ids[0], tenants[1], {"forged": True})
        with pytest.raises(ValueError, match="conflict"):
            await save(ids[0], None, {"forged": True})
        async with factory() as session:
            diagnostic = {"topic": "fixture", "partition": 0, "offset": 1, "tenant_id": tenants[0],
                          "error": "ValueError", "record_sha256": "b" * 64}
            await save_failed_event(session, diagnostic)
            await save_failed_event(session, diagnostic)

        settings = QuerySettings(platform_mode=True, auth_url="http://identity",
            s3_endpoint_url=storage_settings.s3_endpoint_url, s3_access_key=storage_settings.s3_access_key,
            s3_secret_key=storage_settings.s3_secret_key)
        monkeypatch.setattr("query_service.app.api.events.get_settings", lambda: settings)
        application = create_app()
        application.dependency_overrides[query_settings] = lambda: settings
        application.state.query_session_factory = factory

        def identity(request):
            token = request.headers["authorization"]
            selected = request.url.path.split("/")[-2]
            if token not in ("Bearer owner-a", "Bearer owner-b", "Bearer member-a"):
                return httpx.Response(401)
            permitted = str(tenants[1] if token == "Bearer owner-b" else tenants[0])
            if selected != permitted:
                return httpx.Response(404)
            return httpx.Response(200, json={"allowed": True, "tenant_id": selected,
                "user_id": str(uuid.uuid4()), "permission": "tenant.read", "role": "member"})

        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=application), base_url="http://query") as client:
            mock_identity(monkeypatch, identity)
            headers_a = {"authorization": "Bearer owner-a", "x-tenant-id": str(tenants[0])}
            headers_b = {"authorization": "Bearer owner-b", "x-tenant-id": str(tenants[1])}
            assert (await client.get("/api/v1/events")).status_code == 401
            rows = await client.get("/api/v1/events", headers=headers_a)
            assert [r["event_id"] for r in rows.json()] == [ids[0]]
            assert (await client.get(f"/api/v1/events/{ids[0]}", headers=headers_a)).json()["payload"] == {"fixture": True}
            raw = await client.get(f"/api/v1/events/{ids[0]}/raw", headers=headers_a)
            assert raw.status_code == 200 and raw.json() == {"raw_fixture": True}
            assert "location" not in raw.headers and raw.headers["cache-control"] == "no-store"
            assert (await client.get(f"/api/v1/events/{ids[1]}/raw", headers=headers_b)).status_code == 503
            for event_id in (ids[0], ids[2]):
                assert (await client.get(f"/api/v1/events/{event_id}", headers=headers_b)).status_code == 404
                assert (await client.get(f"/api/v1/events/{event_id}/raw", headers=headers_b)).status_code == 404
            assert (await client.get("/api/v1/events", headers={**headers_a, "x-tenant-id": str(tenants[1])})).status_code == 404
            for headers, dlq in ((headers_a, 1), (headers_b, 0)):
                stats = (await client.get("/api/v1/stats", headers=headers)).json()
                assert stats["events_total"] == 1 and stats["dlq_count"] == dlq
                assert sum(stats["events_by_type"].values()) == 1
                assert len(stats["events_by_source"]) == 1
        async with factory() as session:
            row = (await session.execute(select(ProcessedEvent).where(ProcessedEvent.event_id == ids[0]))).scalar_one()
            assert row.tenant_id == tenants[0] and row.payload == {"fixture": True}
            assert row.processed_at <= datetime.now(UTC)
    finally:
        await engine.dispose()
