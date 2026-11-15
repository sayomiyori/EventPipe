# Local hardening contract

Status: implementation approved for isolated local verification. No VPS, domain,
public deployment, new service/UI roadmap or live enrichment calls.

## Authorization and compatibility

- `EVENTPIPE_PLATFORM_MODE`, `QUERY_PLATFORM_MODE`, `TRANSFORM_PLATFORM_MODE` are
  opt-in, default false; all three must match in a platform deployment.
- In platform mode every REST ingest/batch/query/detail/raw/stats request requires
  `Authorization: Bearer ...` and `X-Tenant-ID` UUID. gRPC unary/stream requires
  equivalent `authorization` and `x-tenant-id` metadata. Tenant is a selector;
  canonical context comes only from AuthFortress authorization on every request
  (every streamed event). Ingest requires `tenant.manage`, reads `tenant.read`.
- Forward the bearer to `POST /api/v1/tenants/{tenant_id}/authorize` at the fixed
  configured identity URL; validate the entire positive response, with no cache,
  redirect, retry, local JWT implementation or signing secret. Missing/invalid
  credentials 401, invisible tenant 404, member write 403, unavailable/malformed
  identity 503. gRPC uses equivalent canonical status codes.
- Standalone mode retains existing route and protobuf contracts. Platform gRPC
  identities are server-generated; caller event IDs cannot overwrite other data.
- Tenant UUID is persisted in Kafka envelope, scoped raw-object path and both PG
  records. Every platform read/aggregate filters canonical tenant. Existing rows
  keep NULL tenant and are inaccessible in platform mode. No guessed assignment.
- Platform raw reads proxy authenticated S3 bytes; standalone presign remains.
  Infrastructure credentials remain private. Health/metrics expose operational
  summaries only and are intended for loopback/local infrastructure access.

## Data, diagnostics and recovery

- Alembic owns schema lifecycle: immutable legacy baseline then nullable tenant
  and sanitized failure-record migration. No application `create_all` drift.
  Existing unversioned databases require explicit, exact-schema-checked adoption;
  unexpected schema fails closed. No normal volumes/data are modified by tests.
- Invalid data produces a sanitized DLQ diagnostic (error class, record hash,
  Kafka coordinates, valid tenant UUID only); no original input or exception
  details in DLQ/logs. Persist per-tenant failure counts by Kafka coordinates.
- Transient PG/S3/network errors exhaust bounded immediate retry then escape,
  leaving source offsets uncommitted. Worker restart/replay preserves identity;
  PG write is atomic/idempotent and rejects tenant reassignment. Successful
  persistence or durable DLQ is required before offset commit. DLQ publication is
  at least once; relational failure count is deduplicated by Kafka coordinates.
- User-selected source/type never creates Prometheus series. REST batch count,
  string lengths and REST/gRPC message size are bounded. Geo lookup requires
  explicit opt-in, validates a public IP and HTTPS, and defaults off.

## Acceptance

Two-tenant REST/gRPC ingest and query/detail/raw/stats isolation; owner/member,
invalid/revoked JWT, foreign tenant, identity outage/malformed response; stream
rechecks; legacy isolation; sanitized malformed/validation DLQ; bounded metrics;
real PostgreSQL migration upgrade/downgrade/upgrade and adoption rejection;
Kafka+PG/S3 worker crash/replay/outage proof; full existing tests, Ruff, image
builds and dependency audit. All checks use uniquely named isolated loopback
resources. Public deployment and root runtime activation are out of scope.
