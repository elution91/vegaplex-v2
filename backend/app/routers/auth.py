"""
Auth endpoints — exchange a shared password for a session cookie.
"""
from __future__ import annotations

import hmac

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.config import get_settings

router = APIRouter()


class LoginBody(BaseModel):
    password: str


@router.post("/login")
async def login(body: LoginBody):
    settings = get_settings()
    if not settings.auth_password:
        # Auth disabled — accept any login so frontend can move on
        resp = JSONResponse({"ok": True, "auth": "disabled"})
        return resp

    if not hmac.compare_digest(body.password, settings.auth_password):
        raise HTTPException(status_code=401, detail="Invalid password")

    resp = JSONResponse({"ok": True, "auth": "enabled"})
    # 30-day cookie. HttpOnly omitted so frontend can also send X-Vegaplex-Auth
    # header on subsequent requests if it prefers; cookie is the simpler path.
    resp.set_cookie(
        key="vegaplex_auth",
        value=settings.auth_password,
        max_age=60 * 60 * 24 * 30,
        secure=True,
        samesite="lax",
        httponly=False,
    )
    return resp


@router.get("/status")
async def status():
    settings = get_settings()
    return {"auth_required": bool(settings.auth_password)}
