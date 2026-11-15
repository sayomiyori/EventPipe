"""Exercise EventPipe with real JWT/session/tenant checks in isolated AuthFortress."""

import asyncio
import secrets
import uuid

import grpc
import httpx

from ingest_service.app.generated import event_pb2, event_pb2_grpc
from scripts.verify_eventpipe import INGEST, QUERY
from scripts.verify_local_hardening import wait_event

AUTH = "http://127.0.0.1:55921"


async def main():
    async with httpx.AsyncClient(timeout=15) as client:
        identities = []
        source = "real-identity-" + uuid.uuid4().hex
        for _ in range(2):
            suffix = uuid.uuid4().hex
            email = f"eventpipe-{suffix}@example.com"
            password = secrets.token_urlsafe(32)
            register = await client.post(f"{AUTH}/api/v1/auth/register", json={
                "email": email, "username": f"eventpipe_{suffix}", "password": password,
            })
            assert register.status_code == 200, f"Fixture registration status {register.status_code}"
            login = await client.post(f"{AUTH}/api/v1/auth/login", json={"email": email, "password": password})
            assert login.status_code == 200
            bearer = {"authorization": "Bearer " + login.json()["access_token"]}
            tenant = await client.post(f"{AUTH}/api/v1/tenants", json={"name": f"EventPipe local fixture {suffix}"}, headers=bearer)
            assert tenant.status_code == 201
            identities.append({**bearer, "x-tenant-id": tenant.json()["id"]})
        a, b = identities
        event = {"source": source, "event_type": "identity.fixture", "payload": {"fixture": True}}
        ids = []
        for headers in identities:
            accepted = await client.post(f"{INGEST}/api/v1/events", headers=headers, json=event)
            assert accepted.status_code == 200, f"Real identity ingest status {accepted.status_code}"
            eid = accepted.json()["event_id"]
            ids.append(eid)
            row = await wait_event(client, eid, headers)
            assert row["s3_key"].startswith(f"tenants/{headers['x-tenant-id']}/")
            raw = await client.get(f"{QUERY}/api/v1/events/{eid}/raw", headers=headers)
            assert raw.status_code == 200 and raw.json()["tenant_id"] == headers["x-tenant-id"]
        for suffix in ("", "/raw"):
            assert (await client.get(f"{QUERY}/api/v1/events/{ids[0]}{suffix}", headers=b)).status_code == 404
        foreign = {**b, "x-tenant-id": a["x-tenant-id"]}
        assert (await client.post(f"{INGEST}/api/v1/events", headers=foreign, json=event)).status_code == 404
        assert (await client.get(f"{QUERY}/api/v1/events", headers=foreign)).status_code == 404
        async with grpc.aio.insecure_channel("127.0.0.1:55083") as channel:
            stub = event_pb2_grpc.EventServiceStub(channel)
            reply = await stub.Ingest(event_pb2.Event(source=source, event_type="identity.grpc"), metadata=tuple(a.items()), timeout=10)
            await wait_event(client, reply.event_id, a)
            assert (await client.post(f"{AUTH}/api/v1/auth/logout", headers=a)).status_code == 204
            assert (await client.post(f"{INGEST}/api/v1/events", headers=a, json=event)).status_code == 401
            for path in ("events", f"events/{ids[0]}", f"events/{ids[0]}/raw", "stats"):
                assert (await client.get(f"{QUERY}/api/v1/{path}", headers=a)).status_code == 401
            try:
                await stub.Ingest(event_pb2.Event(source=source, event_type="identity.grpc"), metadata=tuple(a.items()), timeout=10)
            except grpc.aio.AioRpcError as exc:
                assert exc.code() == grpc.StatusCode.UNAUTHENTICATED
            else:
                raise AssertionError("gRPC accepted a revoked real JWT session")
        stats = (await client.get(f"{QUERY}/api/v1/stats", headers=b)).json()
        assert stats["events_by_source"][source] == 1
        assert (await client.post(f"{AUTH}/api/v1/auth/logout", headers=b)).status_code == 204
    print("PASS: real isolated AuthFortress register/login, two tenants, REST/gRPC/raw/stats, foreign 404 and revoked JWT 401")


if __name__ == "__main__":
    asyncio.run(main())
