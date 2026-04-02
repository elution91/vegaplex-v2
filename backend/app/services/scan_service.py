from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncGenerator
from typing import Any

from app.core.errors import safe_run
from app.core.events import sse_complete, sse_error, sse_keepalive, sse_progress

logger = logging.getLogger("vegaplex.scan")

# Active job queues: job_id → asyncio.Queue
_jobs: dict[str, asyncio.Queue] = {}


def _new_job() -> tuple[str, asyncio.Queue]:
    job_id = str(uuid.uuid4())
    q: asyncio.Queue = asyncio.Queue()
    _jobs[job_id] = q
    return job_id, q


def get_job_queue(job_id: str) -> asyncio.Queue | None:
    return _jobs.get(job_id)


def _cleanup_job(job_id: str) -> None:
    _jobs.pop(job_id, None)


async def run_symbol_scan(scanner: Any, symbol: str) -> dict[str, Any]:
    """Single-symbol scan, non-blocking."""
    result = await asyncio.to_thread(safe_run, scanner.scan_symbol_full, symbol, ticker=symbol)
    if result is None:
        return {"symbol": symbol, "opportunities": [], "error": "Scan failed"}
    return {"symbol": symbol, **result}


async def start_universe_scan(scanner: Any, symbols: list[str]) -> str:
    """
    Fire-and-forget universe scan.  Returns job_id immediately.
    Results stream via SSE at /api/scan/stream/{job_id}.
    """
    job_id, q = _new_job()

    async def _worker():
        total = len(symbols)
        all_results: list[dict] = []
        for i, sym in enumerate(symbols, 1):
            result = await asyncio.to_thread(
                safe_run, scanner.scan_symbol_full, sym, ticker=sym
            )
            if result is None:
                await q.put(("error", sym, f"scan failed for {sym}"))
            else:
                row = {"symbol": sym, **result}
                all_results.append(row)
                await q.put(("progress", sym, i, total, row))
        opp_count = sum(len(r.get("opportunities", [])) for r in all_results)
        status = f"Scanned {total} tickers — {opp_count} opportunities found"
        await q.put(("complete", all_results, status))

    asyncio.create_task(_worker())
    return job_id


async def stream_job(job_id: str) -> AsyncGenerator[str, None]:
    """SSE generator — yields formatted event strings until the job completes."""
    q = get_job_queue(job_id)
    if q is None:
        yield sse_error("", f"Unknown job_id: {job_id}")
        return

    try:
        while True:
            try:
                msg = await asyncio.wait_for(q.get(), timeout=25.0)
            except asyncio.TimeoutError:
                yield sse_keepalive()
                continue

            kind = msg[0]
            if kind == "progress":
                _, sym, done, total, result = msg
                yield sse_progress(sym, done, total, result)
            elif kind == "error":
                _, sym, message = msg
                yield sse_error(sym, message)
            elif kind == "complete":
                _, results, status = msg
                yield sse_complete(results, status)
                break
    finally:
        _cleanup_job(job_id)
