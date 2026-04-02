from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, TypeVar

from fastapi import Request
from fastapi.responses import JSONResponse

logger = logging.getLogger("vegaplex")

T = TypeVar("T")


def safe_run(fn: Callable[..., T], *args: Any, ticker: str = "", **kwargs: Any) -> T | None:
    """
    Per-ticker error boundary.  Returns None and logs on any exception so a
    single bad ticker cannot kill a universe scan.
    """
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        label = f"[{ticker}] " if ticker else ""
        logger.warning("%s%s: %s", label, type(exc).__name__, exc, exc_info=False)
        return None


async def http_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Unhandled exception on %s: %s", request.url.path, exc, exc_info=True)
    return JSONResponse(status_code=500, content={"detail": str(exc)})
