from collections.abc import AsyncIterator

from alembic.migration import MigrationContext
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from transform_service.app.config import Settings


def create_engine(settings: Settings):
    return create_async_engine(
        settings.database_url,
        echo=False,
        pool_pre_ping=True,
    )


def create_session_factory(engine):
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def init_db(engine) -> None:
    async with engine.connect() as conn:
        revision = await conn.run_sync(lambda connection: MigrationContext.configure(connection).get_current_revision())
    if revision != "002_tenant_boundary":
        raise RuntimeError("Database migration required: run alembic upgrade head explicitly")


async def get_session(session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        yield session
