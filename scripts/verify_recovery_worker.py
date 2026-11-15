"""Isolated verification hook: hard-exit after PG persistence, before Kafka commit."""

import asyncio
import json
import os

from transform_service.app import consumer
from transform_service.app.main import _async_main

process = consumer.process_kafka_message


async def crash_after_persistence(message, **kwargs):
    await process(message, **kwargs)
    if json.loads(message.value).get("event_id") == os.environ["EVENTPIPE_VERIFY_CRASH_EVENT_ID"]:
        # Test process deliberately dies in the persistence/offset crash window.
        os._exit(137)


if __name__ == "__main__":
    consumer.process_kafka_message = crash_after_persistence
    asyncio.run(_async_main())
