from prometheus_client import CONTENT_TYPE_LATEST, Gauge, Histogram, generate_latest
from starlette.responses import Response

query_duration_seconds = Histogram("query_duration_seconds", "Duration of query endpoint processing")
active_queries = Gauge("active_queries", "Number of active query requests")


def metrics_response() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
