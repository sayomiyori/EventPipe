# Verified defects — 2026-10-03

## 2026-10-12: Images retained root processes and an outdated installer

Actual ingest/query/transform image inspection found UID 0 and pip 25.0.1, the
same version rejected by the current package advisory audit. Upgrade pip before
installing existing requirements and run applications/migrations as UID 10001.
Build-time protobuf generation stays before USER; source files are read-only to
the application user. Runtime storage remains PostgreSQL/S3, with no ownership
changes to existing infrastructure volumes. Verify actual image inventories,
startup, migration and real pipeline/recovery after rebuilding.

## 2026-10-12: Kafka startup collided with a stale ZooKeeper session

After the shared runtime interruption, the preserved test broker exited with
NodeExistsException while ZooKeeper still held the previous ephemeral broker
registration. OOMKilled was false. ZooKeeper expired that session normally;
the subsequent broker start succeeded without deleting data. Set the existing
Kafka service to bounded on-failure:3 restarts in standalone/test Compose.
Readiness is still required before tests or application startup; this does not
hide permanent configuration errors or restart a deliberately stopped broker.

## 2026-10-12: Explicit schema startup procedure

The previous Quick Start bypassed the new required revision check. Documented
startup now validates Compose, starts infrastructure, explicitly upgrades an
empty database through the tools-profile migrate service and starts applications
only after success. Existing unversioned schemas require the checked adoption
script before upgrade; automatic or blind stamping is not supported. Historical
verification remains dated evidence, not a claim of the current runtime rollout.

## 2026-10-09: Startup and current checks

- Main Compose SeaweedFS healthcheck invoked unavailable curl against a 404
  route. Use the image's wget on `http://127.0.0.1:9333/cluster/status`.
  Literal IPv4 is required here: localhost selected an unbound IPv6 listener.
  The exact command passed inside the real SeaweedFS 3.75 image.
- Stop a partially started Kafka producer before retrying known Kafka/network
  failures. `test_failed_start_closes_producer_before_retry` verifies recovery.
- Full real Kafka/PostgreSQL/S3 suite: 27 passed; full service Ruff now clean.
  Remaining historical lint findings were corrected without suppressing rules.
  Independent review approved. An earlier multi-process smoke attempt was
  rejected with `blocked by policy`; the continuation below completed it.
- CI still used unavailable `minio/minio:latest` after the SeaweedFS migration.
  The user applied the reviewed workflow replacement; commit `79ab8dd` reuses
  the existing test Compose, runs all tests and rejects skipped tests.
  GitHub run `37894123954` passed: 27 tests, three builds and cleanup.
- Fresh local image builds encountered a Debian mirror HTTP 503 and then a pip
  resolution failure. Repeating the unchanged builds succeeded; dependency
  versions and Dockerfiles were not changed. The rebuilt stack passed the full
  smoke twice. Expected validation/retry logs belonged to the deliberate DLQ
  probe. See `verification-checkpoint.md` for commands and remaining boundaries.

- Consumer decoded malformed UTF-8 outside its failure handling and assumed decoded
  JSON was an object. Regression-first tests cover malformed UTF-8/list/null DLQ routing.
- Consumer committed source offsets even when DLQ publication failed. The failure
  now escapes without committing; reviewer consumer subset: six tests passed.
- Query raw endpoint changed the presigned S3 host after signing, invalidating SigV4.
  Sign against the public endpoint from the start; builder reported real signature
  smoke failed before and passed after on isolated SeaweedFS S3.

## Regression commands

```powershell
.venv/Scripts/python.exe -m pytest transform_service/tests/test_consumer_failures.py -q --tb=short
# Original reproduction: 4 failed, 2 passed; after correction: 6 passed
.venv/Scripts/python.exe -m scripts.verify_eventpipe
# Original public-host signature assertion failed; after correction: PASS
```

The isolated smoke uses real REST/gRPC, Kafka, PostgreSQL and SeaweedFS S3. Its
signature assertion independently recomputes SigV4 using the returned URL's actual
Host; this catches the defect even if an S3-compatible test server accepts requests
without checking signatures. The worker tests cover successful DLQ publication and
rejection before source offset commit when the DLQ HTTP/Kafka boundary fails.

## 2026-10-04: Local environment and scoped lint corrections

- Docker Desktop's restart caused Windows to reserve TCP ports 58973–59072.
  S3 binding at 59000 failed with a forbidden-socket error. `netsh interface ipv4
  show excludedportrange protocol=tcp` confirmed the range. Only the isolated test
  profile now publishes S3 at 39000; internal endpoint, volumes and user data remain.
- Five Ruff findings in already changed verification files were corrected without
  changing API aliases or pipeline semantics. The full check still reports 13
  existing findings; no blanket formatting or suppression was applied.

Current verification commands/results, public-deployment blockers and the pending
independent reviewer verdict are recorded in `verification-checkpoint.md`.
