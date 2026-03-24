from typing import Any

import httpx

from transform_service.app.config import Settings
from transform_service.app.pipeline.enricher import enrich
from transform_service.app.pipeline.normalizer import normalize
from transform_service.app.pipeline.validator import validate_raw_event, validated_to_dict


async def run_pipeline(
    raw: dict[str, Any],
    *,
    settings: Settings,
    http_client: httpx.AsyncClient | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    validated = validate_raw_event(raw)
    event_dict = validated_to_dict(validated)
    enrichments = await enrich(event_dict, settings=settings, http_client=http_client)
    normalized = normalize(event_dict, enrichments)
    return normalized, enrichments
