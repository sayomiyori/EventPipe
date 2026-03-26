# EventPipe

[![CI](https://github.com/sayomiyori/EventPipe/actions/workflows/ci.yml/badge.svg)](https://github.com/sayomiyori/EventPipe/actions)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue)](#)
[![FastAPI](https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white)](#)
[![Kafka](https://img.shields.io/badge/Kafka-231F20?logo=apachekafka&logoColor=white)](#)
[![gRPC](https://img.shields.io/badge/gRPC-4285F4?logo=google&logoColor=white)](#)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)](#)
[![MinIO](https://img.shields.io/badge/MinIO-C72E49?logo=minio&logoColor=white)](#)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?logo=kubernetes&logoColor=white)](#)
[![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)](#)
[![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?logo=prometheus&logoColor=white)](#)

Microservice ETL pipeline: **Ingest (REST + gRPC) → Kafka → Transform (validate/enrich/normalize) → PostgreSQL + MinIO (S3)**, with Query API, Dead Letter Queue, Prometheus/Grafana monitoring, Docker Compose, and Kubernetes manifests.

## Architecture

```
┌──────────────────┐     ┌────────────────┐     ┌─────────────────┐
│ Ingest Service   │────▶│ Apache Kafka   │────▶│ Transform       │
│ FastAPI          │     │ events.raw     │     │ Service         │
│ REST + gRPC      │     │ events.dlq     │     │ validate →      │
└──────────────────┘     └────────────────┘     │ enrich →        │
                                                │ normalize       │
                                                └────┬───────┬────┘
                                                     │       │
                                                     ▼       ▼
                                              ┌──────────┐ ┌────────┐
                                              │PostgreSQL│ │ MinIO  │
                                              │(results) │ │ (S3)   │
                                              └──────────┘ └────────┘

┌──────────────────┐     ┌─────────────┐
│ Query Service    │     │ Prometheus  │
│ FastAPI          │     │ + Grafana   │
│ REST             │     └─────────────┘
└──────────────────┘
```

## Screenshots

### Docker Compose — all services running
![Docker Services](docs/images/docker-services-phase1.png)

### Ingest API (Swagger)
![Swagger Ingest](docs/images/swagger-ingest.png)

### Query API (Swagger)
![Swagger Query](docs/images/swagger-query.png)

### Kafka Consumer — events.raw messages
![Kafka Consumer](docs/images/kafka-consumer.png)

### Transform Service logs
![Transform Logs](docs/images/transform-logs.png)

### PostgreSQL — processed events
![PostgreSQL Events](docs/images/postgres-events.png)

### MinIO — raw-events bucket
![MinIO Bucket](docs/images/minio-bucket.png)

### Query Stats response
![Stats Response](docs/images/stats-response.png)

### Prometheus Metrics
![Prometheus Metrics](docs/images/prometheus-metrics.png)

### Grafana Dashboard
![Grafana Dashboard](docs/images/grafana-dashboard.png)

### Kubernetes — pods running
![K8s Pods](docs/images/k8s-pods.png)

### Kubernetes — scaled Transform deployment
![K8s Scaled](docs/images/k8s-scaled.png)

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Ingest API | FastAPI (asyncio) + gRPC (grpcio, protobuf) |
| Message broker | Apache Kafka (aiokafka), 3 partitions |
| Transform | Python consumer, pipeline: validate → enrich → normalize |
| Storage (processed) | PostgreSQL 16, SQLAlchemy 2 (asyncpg) |
| Storage (raw) | MinIO (S3-compatible), bucket `raw-events` |
| Dead Letter Queue | Kafka topic `events.dlq` |
| Metrics | Prometheus + Grafana |
| Deployment | Docker Compose, Kubernetes (Minikube) |
| CI | GitHub Actions (ruff + pytest) |

## Architecture Decisions

**Kafka over RabbitMQ** — EventPipe processes high-throughput event streams where ordering matters. Kafka's partitioned log with consumer groups gives replay capability and natural parallelism — Transform workers scale by partition count.

**MinIO for raw storage, PostgreSQL for processed** — raw events are immutable blobs (write-once, read-rarely) → S3-compatible storage. Processed events need filtering, aggregation, joins → relational DB with JSONB.

**Separate Ingest / Transform / Query services** — each has a different scaling profile: Ingest is CPU-light I/O-bound, Transform is CPU-heavy, Query is read-heavy. Independent scaling without affecting other services.

**Dead Letter Queue as separate Kafka topic** — failed events don't block the main pipeline. DLQ enables manual inspection and replay without data loss.

**gRPC alongside REST** — high-throughput internal clients use gRPC (binary, streaming); external clients use REST. Both share the same Kafka producer — no code duplication.

## Quick Start

```bash
# Full stack (Ingest :8013, Query :8020, MinIO :9001, Prometheus :9095, Grafana :3001)
docker compose up -d --build
```

Default credentials:
- **MinIO Console**: `http://localhost:9001` — `minio` / `minio12345`
- **Grafana**: `http://localhost:3001` — `admin` / `admin`
- **Prometheus**: `http://localhost:9095`

## API

### Ingest Service (`http://localhost:8013`)

#### `POST /api/v1/events`

Ingest a single event.

```json
// Request
{
  "source": "payment-service",
  "event_type": "order.created",
  "payload": {"order_id": 42, "amount": 1500},
  "metadata": {"env": "production"}
}

// Response 200
{
  "event_id": "a1b2c3d4-...",
  "status": "accepted"
}
```

#### `POST /api/v1/events/batch`

Ingest multiple events.

```json
// Request
{
  "events": [
    {"source": "app-1", "event_type": "user.created", "payload": {"user_id": 1}, "metadata": {}},
    {"source": "app-2", "event_type": "order.paid", "payload": {"order_id": 2}, "metadata": {}}
  ]
}
```

#### `GET /metrics`

Prometheus metrics (text/plain).

#### `GET /health`

Health check.

---

### Query Service (`http://localhost:8020`)

#### `GET /api/v1/events`

List processed events with filters.

| Parameter | Description |
|-----------|-------------|
| `source` | Filter by source |
| `event_type` | Filter by event type |
| `from` | Start datetime |
| `to` | End datetime |
| `page` | Page number (default: 1) |
| `size` | Page size (default: 20) |

#### `GET /api/v1/events/{event_id}`

Full event details: payload, metadata, enrichments, s3_key.

#### `GET /api/v1/events/{event_id}/raw`

Redirect (307) to a pre-signed MinIO URL (15 min expiry). Downloads the raw JSON event file.

#### `GET /api/v1/stats`

Aggregated statistics.

```json
{
  "events_total": 1500,
  "events_by_source": {"payment-service": 800, "user-service": 700},
  "events_by_type": {"order.created": 500, "user.created": 400, ...},
  "dlq_count": 3
}
```

#### `GET /metrics`

Prometheus metrics (text/plain).

#### `GET /health`

Health check.

## Prometheus Metrics

### Ingest Service

| Metric | Type | Description |
|--------|------|-------------|
| `events_ingested_total` | Counter | Ingested events; labels: `source`, `event_type` |
| `ingest_duration_seconds` | Histogram | Ingest request latency |

### Transform Service

| Metric | Type | Description |
|--------|------|-------------|
| `events_processed_total` | Counter | Successfully processed events |
| `events_failed_total` | Counter | Events sent to DLQ |
| `transform_duration_seconds` | Histogram | Full pipeline latency |
| `kafka_consumer_lag` | Gauge | Consumer group lag |
| `s3_upload_duration_seconds` | Histogram | MinIO upload latency |

### Query Service

| Metric | Type | Description |
|--------|------|-------------|
| `query_duration_seconds` | Histogram | Query request latency |
| `active_queries` | Gauge | Currently executing queries |

## Running Tests

```bash
# Install dependencies
pip install -r ingest_service/requirements.txt
pip install -r transform_service/requirements.txt
pip install -r query_service/requirements.txt

# Generate protobuf
make proto

# Run tests (non-integration)
make test
# or
PYTHONPATH=. python -m pytest -m "not integration" -v
```

## Kubernetes (Minikube)

### Prerequisites

- `kubectl`
- `minikube`

### Deploy

```bash
# 1. Start Minikube
minikube start --memory=4096

# 2. Build images inside Minikube's Docker daemon
eval "$(minikube docker-env)"    # bash
# or: & minikube -p minikube docker-env --shell powershell | Invoke-Expression  # PowerShell

docker build -f ingest_service/Dockerfile -t eventpipe-ingest:latest .
docker build -f transform_service/Dockerfile -t eventpipe-transform:latest .
docker build -f query_service/Dockerfile -t eventpipe-query:latest .

# 3. Enable Ingress
minikube addons enable ingress

# 4. Apply manifests
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/ --recursive
kubectl get pods -n eventpipe

# 5. Access Ingest Service
minikube service ingest-service -n eventpipe
```

### Scaling

```bash
kubectl scale deployment transform --replicas=4 -n eventpipe
kubectl get pods -n eventpipe
```

## Project Structure

```
eventpipe/
├── proto/                    # Protobuf schema
├── ingest_service/           # REST + gRPC → Kafka producer
├── transform_service/        # Kafka consumer → pipeline → PG + S3
├── query_service/            # REST API for reading processed events
├── monitoring/               # Prometheus + Grafana configs
├── k8s/                      # Kubernetes manifests
├── docs/images/              # Screenshots
├── docker-compose.yml
├── Makefile
└── pyproject.toml
```
