import os

from fastapi import HTTPException, Request

# Simple JWT verification (replace with real secret/algorithm in production)
JWT_SECRET = os.environ.get("CIDSTORE_JWT_SECRET", "devsecret")

# Dummy implementation for demonstration
# In production, use PyJWT or equivalent


def verify_jwt(token: str) -> bool:
    # Accept any non-empty token for demo; replace with real JWT validation
    return bool(token)


def jwt_required(request: Request):
    auth: str = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(401, "Missing or invalid Authorization header")
    token = auth.split(" ", 1)[1]
    if not verify_jwt(token):
        raise HTTPException(401, "Invalid JWT token")


# Internal IP check (simple, not production-grade)
def is_internal_ip(request: Request) -> bool:
    client_ip = request.client.host
    return (
        client_ip.startswith("127.")
        or client_ip == "::1"
        or client_ip.startswith("10.")
        or client_ip.startswith("192.168.")
    )


def internal_only(request: Request):
    if not is_internal_ip(request):
        raise HTTPException(403, "Access restricted to internal IPs")
