"""Local verification-only online identity boundary; never deploy as identity."""

from uuid import UUID

from fastapi import FastAPI, HTTPException, Request

app = FastAPI()
TENANT_A = "00000000-0000-4000-8000-000000000001"
TENANT_B = "00000000-0000-4000-8000-000000000002"


@app.post("/api/v1/tenants/{tenant_id}/authorize")
async def authorize(tenant_id: UUID, request: Request):
    bearer = request.headers.get("authorization")
    identities = {"Bearer owner-a": (TENANT_A, "owner"), "Bearer owner-b": (TENANT_B, "owner"),
                  "Bearer member-a": (TENANT_A, "member")}
    if bearer not in identities:
        raise HTTPException(401)
    tenant, role = identities[bearer]
    if str(tenant_id) != tenant:
        raise HTTPException(404)
    permission = (await request.json())["permission"]
    if permission == "tenant.manage" and role != "owner":
        raise HTTPException(403)
    return {"allowed": True, "user_id": "00000000-0000-4000-8000-000000000003",
            "tenant_id": tenant, "permission": permission, "role": role}
