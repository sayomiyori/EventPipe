# EventPipe verification checkpoint — 2026-10-09

Status: **Standalone local ETL, full test suite, lint, review and CI verified.
Shared identity/tenant isolation and public deployment remain separate work.**

## SeaweedFS CI and full runtime continuation — 2026-10-09

The user applied the reviewed workflow after earlier automatic approval
rejections. Commit `79ab8dd` replaces obsolete MinIO GitHub service containers
with the existing `docker-compose.test.yml` Kafka/PostgreSQL/SeaweedFS setup.
All tests run; a JUnit assertion rejects skips. Three Docker builds and
unconditional cleanup remain enabled. No dependencies or application APIs changed.

- Local suite: 27 passed in 13.51 seconds, zero skips.
- Ruff, workflow YAML assertions and `git diff --check`: passed.
- Fresh read-only adversarial review and scoped security pass: approved.
- [GitHub run 37894123954](https://github.com/sayomiyori/EventPipe/actions/runs/37894123954):
  success; 27 passed in 8.47 seconds, all three Docker builds and cleanup passed.
- Local ingest/transform/query images rebuilt; all six healthchecked services
  healthy, Zookeeper running, Kafka initialization exited successfully.
- Full smoke passed twice; the second run took 6.60 seconds. This is a functional
  smoke duration, not a load/capacity measurement.
- The independent reviewer repeated the full smoke successfully (6.85 seconds),
  confirmed the exact green Actions commit and approved the documentation.
- All three built images passed assertions excluding `.env`, `.git` and `.venv`
  under `/app`. Test containers and their network were removed with Compose
  `down`; volumes and the separate NexusCore runtime were preserved.
- Logs showed expected validation retries and failure from the deliberate invalid
  DLQ probe, with no unexplained application errors in the inspected window.

Commands from `D:/Programming/EventPipe`:

```powershell
docker compose -p eventpipe-ci-check -f docker-compose.test.yml config --quiet
docker compose -p eventpipe-ci-check -f docker-compose.test.yml up -d --wait --wait-timeout 240 kafka postgres seaweedfs
# Use the synthetic integration environment from .github/workflows/ci.yml.
.venv/Scripts/python.exe -m pytest -q --junitxml=.pytest_cache/ci.xml
.venv/Scripts/python.exe -c "import xml.etree.ElementTree as E; assert not E.parse('.pytest_cache/ci.xml').findall('.//skipped')"
.venv/Scripts/python.exe -m ruff check ingest_service transform_service query_service scripts
docker compose -p eventpipe-ci-check -f docker-compose.test.yml build ingest transform query
docker compose -p eventpipe-ci-check -f docker-compose.test.yml build ingest
docker compose -p eventpipe-ci-check -f docker-compose.test.yml up -d --no-build --wait --wait-timeout 180 transform query
docker compose -p eventpipe-ci-check -f docker-compose.test.yml up -d --no-build --wait --wait-timeout 180 ingest
.venv/Scripts/python.exe -m scripts.verify_eventpipe
docker compose -p eventpipe-ci-check -f docker-compose.test.yml ps
gh run watch 37894123954 --repo sayomiyori/EventPipe --interval 15 --exit-status
docker compose -p eventpipe-ci-check -f docker-compose.test.yml down
```

The first local ingest build hit a Debian mirror HTTP 503; a later attempt hit
pip dependency resolution failure. An unchanged retry completed successfully.
The tests use retained isolated volumes; no production data or live credentials
are used. Earlier evidence below is historical and does not override this section.

## Historical checkpoint — 2026-10-04

## Checks repeated after resume

Working directory: `D:/Programming/EventPipe`; Python 3.12.10.

```powershell
.venv/Scripts/python.exe -m pytest -m 'not integration' -q --tb=short
# 24 passed, 2 deselected; 1.65 seconds
.venv/Scripts/python.exe -m ruff check ingest_service transform_service query_service scripts --output-format concise
# Exit 1: 13 existing findings remain, down from the recorded baseline of 18
.venv/Scripts/python.exe -m ruff check query_service/app/api/events.py transform_service/tests/test_consumer_failures.py transform_service/tests/test_integration_transform.py scripts --output-format concise
# All checks passed
uvx --from pip-audit pip-audit --path .venv/Lib/site-packages --format json --output docs/dependency-audit.json
# Exit 0: 63 distributions, zero advisory rows; no known vulnerabilities found
git diff --check
# No whitespace errors; Windows line-ending notices only
```

Only lint findings in already changed verification files were corrected: FastAPI
date-query annotations preserve the `from`/`to` API aliases, the consumer uses UTC,
an unused suppression was removed, and integration iteration uses dictionary values.
Remaining Ruff findings affect existing ingest/transform code and tests; the
consumer's existing broad startup-retry exception also remains flagged. No blanket
formatting or lint-rule suppression was applied.

Docker was initially unavailable on resume (`dockerDesktopLinuxEngine` named pipe
missing). After the coordinator started Docker Desktop, all three latest images
were rebuilt and the retained stack was restored. Windows had reserved TCP ports
58973–59072; only the isolated S3 host endpoint moved from 59000 to 39000.

```powershell
docker compose -p eventpipe-verification -f docker-compose.test.yml config --quiet
# Exit 0
docker compose -p eventpipe-verification -f docker-compose.test.yml build ingest transform query
# Exit 0; all three latest service images built
docker compose -p eventpipe-verification -f docker-compose.test.yml up -d --wait --wait-timeout 180
# Exit 0 after the isolated S3 port correction
docker compose -p eventpipe-verification -f docker-compose.test.yml ps --format 'table {{.Service}}\t{{.State}}\t{{.Health}}'
# ingest, transform, query, Kafka, PostgreSQL, SeaweedFS: running/healthy; Zookeeper: running
```

The full suite and real smoke below were repeated with the environment from the
integration section: **26 passed, zero skipped, 36.01 seconds**; smoke **PASS**.
The built services exercised REST/batch/gRPC→Kafka→Transform→PostgreSQL/SeaweedFS
S3→Query, normalization, repeated event identity, validation/not-found, raw JSON
download with independent public-host SigV4 verification, and real Kafka DLQ.

```powershell
.venv/Scripts/python.exe -m pytest -q --tb=short
# 26 passed; 36.01 seconds; no skips
.venv/Scripts/python.exe -m scripts.verify_eventpipe
# PASS: health, metrics, REST, batch, gRPC, duplicate identity, normalization,
# PostgreSQL/query, S3 raw bytes/public signature, validation, not-found, real Kafka DLQ
docker run --rm --network none eventpipe-verification-ingest python -c "from pathlib import Path; bad=[str(p) for p in Path('/app').rglob('*') if p.name in ('.env','.git','.venv')]; assert not bad, bad; print('PASS: ingest image excludes env/git/venv')"
docker run --rm --network none eventpipe-verification-transform python -c "from pathlib import Path; bad=[str(p) for p in Path('/app').rglob('*') if p.name in ('.env','.git','.venv')]; assert not bad, bad; print('PASS: transform image excludes env/git/venv')"
docker run --rm --network none eventpipe-verification-query python -c "from pathlib import Path; bad=[str(p) for p in Path('/app').rglob('*') if p.name in ('.env','.git','.venv')]; assert not bad, bad; print('PASS: query image excludes env/git/venv')"
# All three image exclusion assertions passed
docker compose -p eventpipe-verification -f docker-compose.test.yml logs --since 5m --tail 25 transform query
# HTTP flow completed; worker validation traceback belongs to the deliberate DLQ probe
```

Containers/volumes and generated local test data remain retained. The builder has
released PostgreSQL/Kafka/S3 checks for an independent reviewer; no concurrent
builder DB test is running. Independent review is coordinated separately.

Initial working tree was clean. No commits, pushes, production changes, destructive
database cleanup or migration downgrades were performed. Resources are retained
under `eventpipe-verification` using `docker-compose.test.yml`.

## Historical evidence before pause

- Builder non-integration baseline: **18 passed**.
- Builder full suite with real Kafka/PostgreSQL/S3: **26 passed, zero skipped**, 36.92s.
- Three service Docker images built. Real `scripts/verify_eventpipe.py` smoke passed:
  REST/batch/gRPC→Kafka→Transform→PostgreSQL/S3→Query, normalization, duplicate
  identity, validation/not-found, raw bytes/public signature and real Kafka DLQ.
- Reviewer consumer regression subset: **6 passed**.
- Builder installed-environment dependency audit: exit 0, no known vulnerabilities,
  cache warnings. Audit output is `dependency-audit.json`.
- Baseline full Ruff: **18 errors**, primarily existing issues; not fixed through
  unrelated formatting. Reviewer narrow Ruff found five existing B008/UP017/RUF100/
  BLE001 findings. Do not report lint clean.
- Docker status inspected by coordinator: ingest/query/Kafka/PostgreSQL/SeaweedFS/
  transform healthy; Zookeeper running.

Evidence above comes from builder/reviewer progress messages and coordinator status
inspection. Their final complete command transcript/verdict was interrupted and
must be collected or reproduced on resume before final acceptance.

## Changes

- `transform_service/app/consumer.py`: malformed UTF-8/non-object payloads reach
  DLQ; a failed DLQ publish no longer commits the source offset and silently loses it.
- `query_service/app/api/events.py`: presign with the public S3 endpoint before
  generating the signature; replacing the signed Host afterward invalidated SigV4.
- Updated integration tests, six consumer failure regressions, `.dockerignore`,
  isolated Compose and `scripts/verify_eventpipe.py`.

## Isolated integration environment

Set these test-only endpoints before the full test command. No destructive cleanup
is required; integration events use fresh IDs and retained test data.

```powershell
docker compose -p eventpipe-verification -f docker-compose.test.yml config --quiet
docker compose -p eventpipe-verification -f docker-compose.test.yml ps
$env:EVENTPIPE_KAFKA_BOOTSTRAP_SERVERS = 'localhost:59092'
$env:TRANSFORM_INTEGRATION = '1'
$env:TRANSFORM_KAFKA_BOOTSTRAP_SERVERS = 'localhost:59092'
$env:TRANSFORM_DATABASE_URL = 'postgresql+asyncpg://eventpipe:local-test-only@localhost:55435/eventpipe_test'
$env:TRANSFORM_S3_ENDPOINT_URL = 'http://127.0.0.1:39000'
$env:TRANSFORM_S3_ACCESS_KEY = 'verification'
$env:TRANSFORM_S3_SECRET_KEY = 'local-test-only'
.venv/Scripts/python.exe -m pytest -q --tb=short
.venv/Scripts/python.exe -m scripts.verify_eventpipe
```

## Blocked / not verified

- S3 smoke tested **SeaweedFS 3.75**, used by ordinary Compose. Public MinIO image
  pulls failed (Docker Hub denied; Quay 401); this is not a verified MinIO result.
- No real Alembic migration lifecycle or type-check gate found. Auth/tenant isolation,
  cross-service integration, Kafka offset/crash/outage recovery and public deployment
  remain unverified. REST/query/gRPC are unauthenticated and there is no tenant
  model. Ordinary Compose publishes infrastructure ports broadly and uses example
  credentials; the verification profile binds only to loopback. Transform validation
  errors may include raw input in logs/DLQ. Prometheus source/type labels are supplied
  by callers. These existing security boundaries require a separate integration and
  deployment design; they were not expanded into new auth/tenant functionality.
  No operating-system/image-layer vulnerability audit or Kubernetes check was run.
- Image env/Git/venv exclusions passed after resume. Final independent review,
  crash/outage recovery, public MinIO and the full lint gate remain separate checks.

Resume context: `../../NexusCore/docs/verification-resume-2026-10-03.md`.
