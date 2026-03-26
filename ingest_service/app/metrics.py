from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.responses import Response

events_ingested_total = Counter(
    "events_ingested_total",
    "Count of ingested events",
    labelnames=("source", "event_type"),
)

ingest_duration_seconds = Histogram(
    "ingest_duration_seconds",
    "Duration of ingest endpoints",
)


def metrics_response() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
