from typing import Any


def _strip_dict_strs(d: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in d.items():
        key = k.strip().lower() if isinstance(k, str) else str(k).lower()
        if isinstance(v, str):
            out[key] = v.strip()
        else:
            out[key] = v
    return out


def normalize(event: dict[str, Any], enrichments: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = dict(event)
    out["event_type"] = str(out.get("event_type", "")).strip().lower()
    out["source"] = str(out.get("source", "")).strip()
    out["event_id"] = str(out.get("event_id", "")).strip()
    meta = out.get("metadata") or {}
    if isinstance(meta, dict):
        out["metadata"] = _strip_dict_strs({str(k): v for k, v in meta.items()})
    payload = out.get("payload") or {}
    if isinstance(payload, dict):
        new_payload: dict[str, Any] = {}
        for k, v in payload.items():
            nk = k.strip() if isinstance(k, str) else k
            if isinstance(v, str):
                new_payload[nk] = v.strip()
            else:
                new_payload[nk] = v
        out["payload"] = new_payload
    return out
