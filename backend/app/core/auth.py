"""
Shared-password gate for beta access.

If VEGAPLEX_AUTH_PASSWORD is set, every API request must include
  Header: X-Vegaplex-Auth: <password>
or
  Cookie: vegaplex_auth=<password>

Static files and the /api/auth/login endpoint are exempt so the frontend
can serve the password prompt and exchange it for a cookie.
"""
from __future__ import annotations

import hmac
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware


PUBLIC_PREFIXES = (
    "/api/auth/",
    "/docs",
    "/openapi.json",
    "/redoc",
)


class SharedPasswordMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, password: str):
        super().__init__(app)
        self.password = password

    async def dispatch(self, request: Request, call_next):
        # No password configured → auth disabled (local dev)
        if not self.password:
            return await call_next(request)

        path = request.url.path

        # Static frontend assets and login endpoint are public
        if not path.startswith("/api/") or any(path.startswith(p) for p in PUBLIC_PREFIXES):
            return await call_next(request)

        provided = (
            request.headers.get("x-vegaplex-auth")
            or request.cookies.get("vegaplex_auth")
            or ""
        )
        if not hmac.compare_digest(provided, self.password):
            return JSONResponse(
                status_code=401,
                content={"detail": "Authentication required"},
            )
        return await call_next(request)
