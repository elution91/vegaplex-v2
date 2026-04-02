from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncGenerator
from typing import Any

from app.core.events import sse_complete, sse_error, sse_keepalive, sse_progress

logger = logging.getLogger("vegaplex.earnings")

_jobs: dict[str, asyncio.Queue] = {}


def _new_job() -> tuple[str, asyncio.Queue]:
    job_id = str(uuid.uuid4())
    q: asyncio.Queue = asyncio.Queue()
    _jobs[job_id] = q
    return job_id, q


def get_job_queue(job_id: str) -> asyncio.Queue | None:
    return _jobs.get(job_id)


async def start_earnings_scan(
    settings: Any,
    days_ahead: int = 7,
    min_iv_rv_ratio: float = 0.8,
    data_source: str = "yfinance",
    ibkr_host: str = "127.0.0.1",
    ibkr_port: int = 7497,
) -> str:
    job_id, q = _new_job()

    async def _worker():
        try:
            from earnings_adapter import EarningsAdapter  # noqa: PLC0415

            adapter = EarningsAdapter(
                data_source=data_source,
                ibkr_host=ibkr_host,
                ibkr_port=ibkr_port,
            )
            rows = await asyncio.to_thread(
                adapter.scan,
                days_ahead=days_ahead,
                min_iv_rv_ratio=min_iv_rv_ratio,
                progress_cb=lambda done, total, sym, row: q.put_nowait(
                    ("progress", sym, done, total, row)
                ),
            )
            count = len(rows)
            await q.put(("complete", rows, f"Found {count} earnings setups"))
        except Exception as exc:
            logger.error("Earnings scan failed: %s", exc, exc_info=True)
            await q.put(("error", "", str(exc)))

    asyncio.create_task(_worker())
    return job_id


async def stream_earnings(job_id: str) -> AsyncGenerator[str, None]:
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
                _, sym, done, total, row = msg
                yield sse_progress(sym, done, total, row)
            elif kind == "error":
                _, sym, message = msg
                yield sse_error(sym, message)
            elif kind == "complete":
                _, rows, status = msg
                yield sse_complete(rows, status)
                break
    finally:
        _jobs.pop(job_id, None)
