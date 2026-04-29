from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
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


def _result_to_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten EarningsAdapter.scan() result → list of row dicts for the frontend."""
    stock_metrics = result.get("stock_metrics", {})
    timing        = result.get("timing", {})
    iron_flies    = result.get("iron_flies", {})
    rows: list[dict[str, Any]] = []

    def _build_row(ticker: str, tier: str) -> dict[str, Any]:
        m   = stock_metrics.get(ticker, {})
        tmg = timing.get(ticker, {})
        ed  = tmg.get("earnings_date", "")
        days_to: int | None = None
        if ed:
            try:
                days_to = (datetime.strptime(ed, "%Y-%m-%d").date() - datetime.today().date()).days
            except ValueError:
                pass

        iron_fly_data = iron_flies.get(ticker)
        payoff_chart: dict | None = None
        if iron_fly_data and isinstance(iron_fly_data, dict) and not iron_fly_data.get("error"):
            try:
                from app.services.chart_service import get_iron_fly_payoff_chart  # noqa: PLC0415
                spot = float(m.get("price") or 100)
                payoff_chart = get_iron_fly_payoff_chart(iron_fly_data, spot) or None
            except Exception:
                pass

        return {
            "ticker":        ticker,
            "date":          ed,
            "days":          days_to,
            "tier":          tier,
            "price":         m.get("price"),
            "iv_rv_ratio":   m.get("iv_rv_ratio"),
            "slope":         m.get("term_slope"),
            "win_rate":      m.get("win_rate"),
            "bennett_move":  m.get("bennett_move_pct"),
            "rich":          m.get("richness_ratio"),
            "structure":     m.get("structure_rec", m.get("structure", "")),
            "spread_signal": m.get("spread_signal", ""),
            "confidence":    m.get("confidence"),
            "iron_fly":      iron_fly_data,
            "payoff_chart":  payoff_chart,
            "direction":     tmg.get("timing", ""),
        }

    for ticker in result.get("recommended", {}):
        rows.append(_build_row(ticker, "recommended"))

    for item in result.get("near_misses", []):
        ticker = item[0] if isinstance(item, (list, tuple)) else item
        rows.append(_build_row(ticker, "near_miss"))

    return rows


async def start_earnings_scan(
    settings: Any,
    days_ahead: int = 7,
    min_iv_rv_ratio: float = 0.8,
    data_source: str = "yfinance",
    ibkr_host: str = "127.0.0.1",
    ibkr_port: int = 7497,
) -> str:
    job_id, q = _new_job()

    async def _worker() -> None:
        try:
            from earnings_adapter import EarningsAdapter  # noqa: PLC0415

            adapter = EarningsAdapter()

            scan_kwargs: dict[str, Any] = {"days_ahead": days_ahead}
            if data_source == "ibkr":
                scan_kwargs["ibkr_host"] = ibkr_host
                scan_kwargs["ibkr_port"] = ibkr_port

            raw = await asyncio.to_thread(adapter.scan, **scan_kwargs)
            rows = _result_to_rows(raw)

            # Emit all rows as individual progress events so the UI streams in
            total = len(rows)
            for i, row in enumerate(rows, 1):
                await q.put(("progress", row.get("ticker", ""), i, total, row))

            await q.put(("complete", rows, f"Found {total} earnings setups"))
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
                msg = await asyncio.wait_for(q.get(), timeout=30.0)
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
