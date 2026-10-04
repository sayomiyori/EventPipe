# Verified defects — 2026-10-03

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
