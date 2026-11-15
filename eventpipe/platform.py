"""Online authorization shared by EventPipe REST and gRPC boundaries."""

from uuid import UUID

import httpx
from fastapi import HTTPException


async def authorize_tenant(
    *, platform_mode: bool, auth_url: str, authorization: str | None,
    tenant_selector: str | None, permission: str,
) -> UUID | None:
    if not platform_mode:
        return None
    if (not authorization or not authorization.startswith("Bearer ")
            or not authorization[7:].strip() or len(authorization) > 8192
            or not authorization.isascii() or "\n" in authorization or "\r" in authorization):
        raise HTTPException(401, "invalid bearer credentials")
    try:
        tenant_id = UUID(tenant_selector or "")
    except ValueError:
        raise HTTPException(422, "invalid tenant selector") from None
    if not auth_url:
        raise HTTPException(503, "identity service unavailable")
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(5, connect=2), follow_redirects=False, trust_env=False,
        ) as client:
            response = await client.post(
                f"{auth_url.rstrip('/')}/api/v1/tenants/{tenant_id}/authorize",
                headers={"Authorization": authorization}, json={"permission": permission},
            )
        if response.status_code in (401, 403, 404):
            raise HTTPException(response.status_code, "tenant authorization denied")
        if response.status_code != 200:
            raise ValueError("unexpected identity status")
        response.raise_for_status()
        data = response.json()
        if (data.get("allowed") is not True or UUID(data["tenant_id"]) != tenant_id
                or data.get("permission") != permission or data.get("role") not in ("owner", "member")
                or (permission == "tenant.manage" and data["role"] != "owner")):
            raise ValueError("invalid authorization context")
        UUID(data["user_id"])
        return UUID(data["tenant_id"])
    except (httpx.HTTPError, ValueError, KeyError, TypeError, AttributeError):
        raise HTTPException(503, "identity service unavailable") from None
