"""Verify platform isolation and recovery only in a uniquely named test stack."""

import argparse
import asyncio
import json
import os
import re
import subprocess
import time
import uuid
from pathlib import Path

import grpc
import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

from ingest_service.app.generated import event_pb2, event_pb2_grpc
from ingest_service.tests.identity_stub import TENANT_A, TENANT_B
from scripts.verify_eventpipe import INGEST, KAFKA, QUERY

ROOT = Path(__file__).resolve().parents[1]


def compose(project, *arguments, expected=0):
    if not re.fullmatch(r"eventpipe-local-closure-[a-z0-9-]+", project):
        raise ValueError("Only an isolated closure test project is allowed")
    if os.environ.get("EVENTPIPE_TEST_PROJECT") != project:
        raise ValueError("The fixture image project must match the isolated target")
    result = subprocess.run([
        "docker", "compose", "-p", project, "-f", str(ROOT / "docker-compose.test.yml"),
        "-f", str(ROOT / "docker-compose.platform-test.yml"), *arguments,
    ], cwd=ROOT, capture_output=True, text=True, timeout=120, check=False)
    assert result.returncode == expected, f"Controlled test command failed: {arguments[:2]}"
    return result


async def wait_until(check, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = await check()
        if result:
            return result
        await asyncio.sleep(0.2)
    raise AssertionError("Controlled verification did not reach expected state")


async def wait_event(client, event_id, headers):
    async def check():
        response = await client.get(f"{QUERY}/api/v1/events/{event_id}", headers=headers)
        assert response.status_code in (200, 404), f"Unexpected query status {response.status_code}"
        return response.json() if response.status_code == 200 else None
    return await wait_until(check)


async def main(project, recovery):
    headers_a = {"authorization": "Bearer owner-a", "x-tenant-id": TENANT_A}
    headers_b = {"authorization": "Bearer owner-b", "x-tenant-id": TENANT_B}
    source = "platform-fixture-" + uuid.uuid4().hex
    async with httpx.AsyncClient(timeout=10) as client:
        event = {"source": source, "event_type": "platform.fixture", "payload": {"value": " fixture "}}
        for path in ("events", "events/batch"):
            body = event if path == "events" else {"events": [event]}
            assert (await client.post(f"{INGEST}/api/v1/{path}", json=body)).status_code == 401
            assert (await client.post(f"{INGEST}/api/v1/{path}", json=body, headers={
                **headers_a, "authorization": "Bearer member-a"})).status_code == 403
        assert (await client.get(f"{QUERY}/api/v1/events", headers={**headers_a, "x-tenant-id": TENANT_B})).status_code == 404
        accepted = await client.post(f"{INGEST}/api/v1/events", json=event, headers=headers_a)
        assert accepted.status_code == 200
        event_a = accepted.json()["event_id"]
        accepted_b = await client.post(f"{INGEST}/api/v1/events", json=event, headers=headers_b)
        assert accepted_b.status_code == 200
        event_b = accepted_b.json()["event_id"]
        batch = await client.post(f"{INGEST}/api/v1/events/batch", json={"events": [event, event]}, headers=headers_a)
        assert batch.status_code == 200
        row = await wait_event(client, event_a, headers_a)
        assert row["payload"]["value"] == "fixture"
        assert row["s3_key"].startswith(f"tenants/{TENANT_A}/")
        await wait_event(client, event_b, headers_b)
        for eid in batch.json()["event_ids"]:
            await wait_event(client, eid, headers_a)
        for eid, wrong_headers in ((event_a, headers_b), (event_b, headers_a)):
            for suffix in ("", "/raw"):
                assert (await client.get(f"{QUERY}/api/v1/events/{eid}{suffix}", headers=wrong_headers)).status_code == 404
                assert (await client.get(f"{QUERY}/api/v1/events/{eid}{suffix}")).status_code == 401
        raw = await client.get(f"{QUERY}/api/v1/events/{event_a}/raw", headers=headers_a)
        assert raw.status_code == 200 and "location" not in raw.headers
        assert raw.headers["cache-control"] == "no-store"
        assert raw.json()["tenant_id"] == TENANT_A and raw.json()["payload"]["value"] == " fixture "
        rows_a = (await client.get(f"{QUERY}/api/v1/events", headers=headers_a, params={"source": source})).json()
        rows_b = (await client.get(f"{QUERY}/api/v1/events", headers=headers_b, params={"source": source})).json()
        assert len(rows_a) == 3 and len(rows_b) == 1
        stats_a = (await client.get(f"{QUERY}/api/v1/stats", headers=headers_a)).json()
        stats_b = (await client.get(f"{QUERY}/api/v1/stats", headers=headers_b)).json()
        assert stats_a["events_by_source"][source] == 3 and stats_b["events_by_source"][source] == 1
        assert (await client.get(f"{QUERY}/api/v1/stats")).status_code == 401
        async with grpc.aio.insecure_channel("127.0.0.1:55083") as channel:
            stub = event_pb2_grpc.EventServiceStub(channel)
            grpc_event = event_pb2.Event(event_id=event_b, source=source, event_type="platform.grpc")
            with_error = False
            try:
                await stub.Ingest(grpc_event, timeout=5)
            except grpc.aio.AioRpcError as exc:
                assert exc.code() == grpc.StatusCode.UNAUTHENTICATED
                with_error = True
            assert with_error
            result = await stub.Ingest(grpc_event, timeout=5, metadata=tuple(headers_a.items()))
            assert result.event_id != event_b
            await wait_event(client, result.event_id, headers_a)
        compose(project, "stop", "--timeout", "5", "identity")
        try:
            assert (await client.post(f"{INGEST}/api/v1/events", json=event, headers=headers_a)).status_code == 503
            for path in ("events", f"events/{event_a}", f"events/{event_a}/raw", "stats"):
                assert (await client.get(f"{QUERY}/api/v1/{path}", headers=headers_a)).status_code == 503
        finally:
            compose(project, "start", "identity")
        print("PASS: real platform REST/batch/gRPC, two tenants, owner/member, raw proxy, stats, online auth outage")
        if recovery:
            await verify_recovery(project, client, headers_a, source)


async def verify_recovery(project, client, headers, source):
    observer = AIOKafkaConsumer(bootstrap_servers=KAFKA, group_id="transform-workers", enable_auto_commit=False)
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA)
    await observer.start()
    await producer.start()
    try:
        compose(project, "stop", "--timeout", "5", "transform")
        eid = str(uuid.uuid4())
        body = {"event_id": eid, "tenant_id": TENANT_A, "source": source,
                "event_type": "platform.crash", "timestamp": "2026-10-11T00:00:00Z",
                "payload": {"probe": "crash-after-persistence"}, "metadata": {}}
        record = await producer.send_and_wait("events.raw", json.dumps(body).encode(), key=b"platform.crash")
        tp = TopicPartition(record.topic, record.partition)
        compose(project, "run", "--rm", "--no-deps", "-T", "-e", f"EVENTPIPE_VERIFY_CRASH_EVENT_ID={eid}",
                "transform", "python", "/verification/verify_recovery_worker.py", expected=137)
        persisted = await wait_event(client, eid, headers)
        committed = await observer.committed(tp)
        assert committed is None or committed <= record.offset, "Crash committed source offset too early"
        compose(project, "start", "transform")

        async def committed_after_restart():
            value = await observer.committed(tp)
            return value is not None and value >= record.offset + 1

        await wait_until(committed_after_restart)
        replayed = await wait_event(client, eid, headers)
        assert replayed == persisted, "Crash replay changed the persisted result"
        print("PASS: hard crash after PG/S3 persistence left offset uncommitted; restart replayed once without changing row")

        for service in ("postgres", "seaweedfs"):
            compose(project, "stop", "--timeout", "5", service)
            try:
                eid = str(uuid.uuid4())
                raw = {**body, "event_id": eid, "event_type": f"platform.outage.{service}", "payload": {"probe": service}}
                record = await producer.send_and_wait("events.raw", json.dumps(raw).encode(), key=b"platform.outage")
                tp = TopicPartition(record.topic, record.partition)

                async def worker_stopped():
                    result = compose(project, "ps", "-q", "--status", "running", "transform")
                    return not result.stdout.strip()

                await wait_until(worker_stopped, timeout=60)
                committed = await observer.committed(tp)
                assert committed is None or committed <= record.offset
            finally:
                compose(project, "up", "-d", "--wait", "--wait-timeout", "60", service)
                compose(project, "start", "transform")
            recovered = await wait_event(client, eid, headers)
            assert recovered["payload"] == {"probe": service}
            raw = await client.get(f"{QUERY}/api/v1/events/{eid}/raw", headers=headers)
            assert raw.status_code == 200 and raw.json()["event_id"] == eid
            print(f"PASS: real {service} outage kept source offset; restored storage and worker recovered original event")

        compose(project, "stop", "--timeout", "5", "kafka")
        try:
            response = await client.post(f"{INGEST}/api/v1/events", headers=headers, json={
                "source": source, "event_type": "platform.kafka-outage", "payload": {}})
            assert response.status_code == 503, "Broker outage did not fail within bounded admission timeout"
        finally:
            compose(project, "up", "-d", "--wait", "--wait-timeout", "90", "kafka")
            compose(project, "start", "transform")
        response = await client.post(f"{INGEST}/api/v1/events", headers=headers, json={
            "source": source, "event_type": "platform.broker-restored", "payload": {}})
        assert response.status_code == 200
        await wait_event(client, response.json()["event_id"], headers)
        print("PASS: real Kafka outage returned bounded 503; broker restart restored durable admission and processing")
    finally:
        await producer.stop()
        await observer.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--recovery", action="store_true")
    args = parser.parse_args()
    asyncio.run(main(args.project, args.recovery))
