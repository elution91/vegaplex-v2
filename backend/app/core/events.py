from __future__ import annotations

import json
from typing import Any


def sse_event(event: str, data: Any) -> str:
    """Format a single SSE frame."""
    payload = json.dumps(data, default=str)
    return f"event: {event}\ndata: {payload}\n\n"


def sse_progress(ticker: str, done: int, total: int, result: Any) -> str:
    return sse_event("progress", {"ticker": ticker, "done": done, "total": total, "result": result})


def sse_complete(results: list[Any], status: str) -> str:
    return sse_event("complete", {"results": results, "status": status})


def sse_error(ticker: str, message: str) -> str:
    return sse_event("error", {"ticker": ticker, "message": message})


def sse_keepalive() -> str:
    return ": keepalive\n\n"
