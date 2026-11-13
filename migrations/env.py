import asyncio

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine

from transform_service.app.config import get_settings
from transform_service.app.models.processed_event import Base

config = context.config


def run_migrations(connection):
    context.configure(connection=connection, target_metadata=Base.metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def online():
    engine = create_async_engine(get_settings().database_url)
    async with engine.connect() as connection:
        await connection.run_sync(run_migrations)
    await engine.dispose()


if connection := config.attributes.get("connection"):
    run_migrations(connection)
else:
    asyncio.run(online())
