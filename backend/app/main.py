from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.config import get_settings
from app.core.auth import SharedPasswordMiddleware
from app.core.errors import http_exception_handler

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("vegaplex")


# ── Ensure analytics/ is importable ──────────────────────────────────────────
_analytics_dir = Path(__file__).parent.parent / "analytics"
if str(_analytics_dir) not in sys.path:
    sys.path.insert(0, str(_analytics_dir))


# ── Lifespan: start/stop scheduler, pre-warm singletons ──────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("νegaPlex v2 starting")
    yield
    logger.info("νegaPlex v2 shutdown")


# ── App factory ───────────────────────────────────────────────────────────────
def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title="νegaPlex API",
        version="2.0.0",
        description="Volatility surface intelligence — FastAPI backend",
        lifespan=lifespan,
    )

    # CORS — allow Vite dev server + production frontend
    cors_list = settings.cors_origins_list()
    logger.info(f"CORS allow_origins = {cors_list}")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_list,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Shared-password gate (no-op when VEGAPLEX_AUTH_PASSWORD is empty)
    app.add_middleware(SharedPasswordMiddleware, password=settings.auth_password)

    app.add_exception_handler(Exception, http_exception_handler)

    # ── Routers ───────────────────────────────────────────────────────────────
    from app.routers import (  # noqa: PLC0415
        auth,
        broker,
        earnings,
        macro_events,
        radar,
        regime,
        scan,
        scheduler,
        surface,
        vix,
    )

    app.include_router(auth.router,      prefix="/api/auth",       tags=["auth"])
    app.include_router(scan.router,      prefix="/api/scan",       tags=["scan"])
    app.include_router(surface.router,   prefix="/api/surface",    tags=["surface"])
    app.include_router(regime.router,    prefix="/api/regime",     tags=["regime"])
    app.include_router(radar.router,     prefix="/api/radar",      tags=["radar"])
    app.include_router(vix.router,       prefix="/api/vix",        tags=["vix"])
    app.include_router(earnings.router,  prefix="/api/earnings",   tags=["earnings"])
    app.include_router(broker.router,    prefix="/api/broker",     tags=["broker"])
    app.include_router(scheduler.router, prefix="/api/scheduler",  tags=["scheduler"])
    app.include_router(macro_events.router, prefix="/api/macro-events", tags=["macro-events"])

    # ── Serve built frontend (production) ────────────────────────────────────
    frontend_dist = Path(__file__).parent.parent.parent / "frontend" / "dist"
    if frontend_dist.exists():
        app.mount("/", StaticFiles(directory=str(frontend_dist), html=True), name="static")

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)
