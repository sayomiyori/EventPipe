from datetime import UTC, datetime
from ipaddress import ip_address
from typing import Any

import httpx

from transform_service.app.config import Settings


def _extract_ip(event: dict[str, Any]) -> str | None:
    payload = event.get("payload") or {}
    metadata = event.get("metadata") or {}
    for key in ("client_ip", "ip", "remote_addr"):
        v = payload.get(key) or metadata.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _parse_timestamp(ts: str) -> datetime:
    raw = (ts or "").strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


async def lookup_geo_ip(
    ip: str,
    client: httpx.AsyncClient,
    timeout_seconds: float,
) -> dict[str, Any]:
    parsed = ip_address(ip)
    if not parsed.is_global:
        raise ValueError("geo enrichment requires a public IP")
    url = f"https://ip-api.com/json/{parsed}"
    resp = await client.get(url, params={"fields": "status,country,countryCode,city"}, timeout=timeout_seconds)
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "success":
        return {"error": "geo_lookup_failed", "ip": ip}
    return {
        "country": data.get("country"),
        "country_code": data.get("countryCode"),
        "city": data.get("city"),
        "source": "ip-api.com",
    }


async def enrich(
    event: dict[str, Any],
    *,
    settings: Settings,
    http_client: httpx.AsyncClient | None,
) -> dict[str, Any]:
    enrichments: dict[str, Any] = {}
    try:
        enrichments["timestamp_normalized"] = _parse_timestamp(str(event.get("timestamp", ""))).isoformat()
    except (ValueError, TypeError):
        enrichments["timestamp_normalized"] = None

    ip = _extract_ip(event)
    if not ip:
        enrichments["geo"] = None
        return enrichments

    if http_client is None or not settings.geo_ip_enabled:
        enrichments["geo"] = {"skipped": True, "ip": ip}
        return enrichments

    try:
        enrichments["geo"] = await lookup_geo_ip(ip, http_client, settings.geo_ip_timeout_seconds)
    except Exception as exc:  # noqa: BLE001 — enrichment is best-effort
        enrichments["geo"] = {"error": type(exc).__name__, "ip": ip}

    return enrichments
