from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from ingest_service.app.main import create_app


class FakeKafkaProducer:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []
        self._event_id = 0

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    def new_event_id(self) -> str:
        self._event_id += 1
        return f"00000000-0000-4000-8000-{self._event_id:012d}"

    async def publish_raw_event(self, *, event_type: str, body: dict[str, Any]) -> None:
        self.published.append((event_type, body))


@pytest.fixture
def fake_producer() -> FakeKafkaProducer:
    return FakeKafkaProducer()


@pytest_asyncio.fixture
async def rest_client(fake_producer: FakeKafkaProducer) -> AsyncIterator[tuple[AsyncClient, FakeKafkaProducer]]:
    application = create_app()
    application.state.producer = fake_producer
    transport = ASGITransport(app=application)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, fake_producer


@pytest.fixture
def mock_aiokafka_producer() -> MagicMock:
    m = MagicMock()
    m.start = AsyncMock()
    m.stop = AsyncMock()
    m.send_and_wait = AsyncMock()
    return m
