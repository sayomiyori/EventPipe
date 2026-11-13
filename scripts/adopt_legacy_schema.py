"""Explicitly stamp only an exact legacy schema; never migrate or assign tenants."""

import asyncio

from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text

from transform_service.app.config import get_settings
from transform_service.app.db.session import create_engine


def adopt(connection):
    inspector = inspect(connection)
    if set(inspector.get_table_names()) != {"processed_events"}:
        raise RuntimeError("Legacy adoption rejected: unexpected tables or existing revision")
    if inspector.get_view_names() or inspector.get_sequence_names():
        raise RuntimeError("Legacy adoption rejected: unexpected objects")
    expected = {
        "id": ("UUID", False, None), "event_id": ("VARCHAR(64)", False, None),
        "source": ("VARCHAR(255)", False, None), "event_type": ("VARCHAR(255)", False, None),
        "payload": ("JSONB", False, None), "metadata": ("JSONB", False, None),
        "s3_key": ("TEXT", False, None),
        "processed_at": ("TIMESTAMP WITH TIME ZONE", False, "now()"),
        "enrichments": ("JSONB", False, None),
    }
    columns = inspector.get_columns("processed_events")
    actual = {c["name"]: (str(c["type"].compile(dialect=connection.dialect)), c["nullable"], c["default"]) for c in columns}
    indexes = inspector.get_indexes("processed_events")
    actual_indexes = {(i["name"], tuple(i["column_names"]), i["unique"]) for i in indexes}
    expected_indexes = {("ix_processed_events_event_id", ("event_id",), True),
                        ("ix_processed_events_event_type", ("event_type",), False)}
    if (actual != expected or actual_indexes != expected_indexes
            or any(c.get("identity") or c.get("computed") for c in columns)
            or any(i.get("dialect_options", {}).get("postgresql_where")
                   or i.get("dialect_options", {}).get("postgresql_include") for i in indexes)
            or inspector.get_pk_constraint("processed_events")["constrained_columns"] != ["id"]
            or inspector.get_foreign_keys("processed_events")
            or inspector.get_unique_constraints("processed_events")
            or inspector.get_check_constraints("processed_events")):
        raise RuntimeError("Legacy adoption rejected: schema mismatch")
    extras = connection.scalar(text("""
        SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
          WHERE n.nspname='public') OR EXISTS
        (SELECT 1 FROM pg_trigger WHERE NOT tgisinternal) OR EXISTS
        (SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
          WHERE n.nspname='public' AND t.typtype='e') OR EXISTS
        (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname='public' AND c.relkind IN ('m','f','p'))
    """))
    if extras:
        raise RuntimeError("Legacy adoption rejected: unexpected schema objects")
    config = Config("alembic.ini")
    config.attributes["connection"] = connection
    command.stamp(config, "001_legacy_baseline")


async def main():
    engine = create_engine(get_settings())
    try:
        async with engine.begin() as connection:
            await connection.run_sync(adopt)
    finally:
        await engine.dispose()
    print("PASS: exact legacy schema adopted at 001_legacy_baseline; rows unchanged")


if __name__ == "__main__":
    asyncio.run(main())
