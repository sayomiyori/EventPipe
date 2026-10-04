"""Verify the isolated EventPipe stack without external HTTP calls or deleting data."""

import asyncio
import hashlib
import hmac
import json
import time
import uuid
from urllib.parse import parse_qs, quote, urlparse

import grpc
import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from google.protobuf import struct_pb2

from ingest_service.app.generated import event_pb2, event_pb2_grpc

INGEST = "http://127.0.0.1:38083"
QUERY = "http://127.0.0.1:38085"
KAFKA = "localhost:59092"


def verify_public_signature(url):
    """Recompute the SigV4 signature using the public request's actual Host."""
    parts = urlparse(url)
    query = parse_qs(parts.query)
    provided = query.pop("X-Amz-Signature")[0]
    credential = query["X-Amz-Credential"][0].split("/")
    canonical_query = "&".join(
        f"{quote(k, safe='-_.~')}={quote(v[0], safe='-_.~')}" for k, v in sorted(query.items())
    )
    canonical = f"GET\n{parts.path}\n{canonical_query}\nhost:{parts.netloc}\n\nhost\nUNSIGNED-PAYLOAD"
    scope = "/".join(credential[1:])
    to_sign = f"AWS4-HMAC-SHA256\n{query['X-Amz-Date'][0]}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
    key = b"AWS4local-test-only"
    for value in credential[1:]:
        key = hmac.new(key, value.encode(), hashlib.sha256).digest()
    assert hmac.compare_digest(provided, hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()), (
        "Raw download signature does not authenticate its public Host"
    )


async def wait_event(client, event_id):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        response = await client.get(f"{QUERY}/api/v1/events/{event_id}")
        if response.status_code == 200:
            return response.json()
        assert response.status_code == 404
        await asyncio.sleep(0.2)
    raise AssertionError("Transform did not persist the event within 30 seconds")


async def main():
    source = f"verification-{uuid.uuid4().hex}"
    async with httpx.AsyncClient(timeout=10) as client:
        for base in (INGEST, QUERY):
            assert (await client.get(f"{base}/health")).status_code == 200
            assert (await client.get(f"{base}/metrics")).status_code == 200
        invalid = await client.post(f"{INGEST}/api/v1/events", json={"source": source})
        assert invalid.status_code == 422
        response = await client.post(f"{INGEST}/api/v1/events", json={
            "source": source, "event_type": " Verification.Rest ", "payload": {" value ": " cleaned "},
            "metadata": {" ENV ": " local "},
        })
        assert response.status_code == 200
        rest_id = response.json()["event_id"]
        row = await wait_event(client, rest_id)
        assert row["event_type"] == "verification.rest"
        assert row["payload"] == {"value": "cleaned"}
        assert row["metadata"] == {"env": "local"}
        assert row["enrichments"]["geo"] is None

        batch = await client.post(f"{INGEST}/api/v1/events/batch", json={"events": [
            {"source": source, "event_type": "verification.batch", "payload": {"index": i}} for i in range(2)
        ]})
        assert batch.status_code == 200 and batch.json()["count"] == 2
        for event_id in batch.json()["event_ids"]:
            await wait_event(client, event_id)

        grpc_id = str(uuid.uuid4())
        async with grpc.aio.insecure_channel("127.0.0.1:55083") as channel:
            stub = event_pb2_grpc.EventServiceStub(channel)
            payload = struct_pb2.Struct()
            payload.update({"origin": "grpc"})
            event = event_pb2.Event(event_id=grpc_id, source=source, event_type="verification.grpc", payload=payload)
            for _ in range(2):
                result = await stub.Ingest(event, timeout=10)
                assert result.status == "accepted" and result.event_id == grpc_id
            grpc_row = await wait_event(client, grpc_id)
            assert grpc_row["payload"] == {"origin": "grpc"}
            try:
                await stub.Ingest(event_pb2.Event(source=source), timeout=10)
            except grpc.aio.AioRpcError as exc:
                assert exc.code() == grpc.StatusCode.INVALID_ARGUMENT
            else:
                raise AssertionError("gRPC accepted a missing event_type")

        rows = (await client.get(f"{QUERY}/api/v1/events", params={"source": source})).json()
        assert len(rows) == 4
        assert sum(row["event_id"] == grpc_id for row in rows) == 1
        assert (await client.get(f"{QUERY}/api/v1/events", params={"size": 201})).status_code == 422
        assert (await client.get(f"{QUERY}/api/v1/events/{uuid.uuid4()}")).status_code == 404

        raw = await client.get(f"{QUERY}/api/v1/events/{rest_id}/raw")
        assert raw.status_code == 307
        verify_public_signature(raw.headers["location"])
        downloaded = await client.get(raw.headers["location"])
        assert downloaded.status_code == 200
        original = downloaded.json()
        assert original["event_id"] == rest_id
        assert original["payload"] == {" value ": " cleaned "}
        stats = await client.get(f"{QUERY}/api/v1/stats")
        assert stats.status_code == 200 and stats.json()["events_by_source"][source] == 4

        probe = uuid.uuid4().hex
        consumer = AIOKafkaConsumer("events.dlq", bootstrap_servers=KAFKA, group_id=f"verify-{probe}",
                                    auto_offset_reset="earliest", enable_auto_commit=False)
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA)
        await consumer.start()
        await producer.start()
        try:
            await producer.send_and_wait("events.raw", json.dumps({"verification_probe": probe}).encode())
            deadline = time.monotonic() + 30
            found = False
            while time.monotonic() < deadline and not found:
                messages = await consumer.getmany(timeout_ms=1000)
                found = any(json.loads(m.value).get("original", {}).get("verification_probe") == probe
                            for batch in messages.values() for m in batch)
            assert found, "Invalid event did not reach real Kafka DLQ"
        finally:
            await producer.stop()
            await consumer.stop()
    print("PASS: health, metrics, REST, batch, gRPC, duplicate identity, normalization, PostgreSQL/query, "
          "S3 raw bytes/public signature, validation, not-found, real Kafka DLQ")


if __name__ == "__main__":
    asyncio.run(main())
