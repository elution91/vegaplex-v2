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


def _synthetic_legs(opp: dict[str, Any]) -> list[dict[str, Any]]:
    """Build minimal leg list from opportunity strikes/prices when trade_structure absent."""
    strikes = opp.get("strikes") or {}
    prices = opp.get("prices") or {}
    expiry = opp.get("expiry", "")
    subtype = opp.get("subtype", "")
    opp_type = opp.get("type", "")

    # Infer option type from subtype string
    opt_type = "put" if "put" in subtype else "call"

    legs: list[dict[str, Any]] = []
    # sell leg
    sell_k = strikes.get("sell") or strikes.get("short")
    sell_p = prices.get("sell_mid") or prices.get("short_mid", 0)
    if sell_k is not None:
        legs.append({"action": "SELL", "type": opt_type, "strike": sell_k,
                     "expiry": expiry, "price": sell_p, "contracts": 1})
    # buy leg
    buy_k = strikes.get("buy") or strikes.get("long")
    buy_p = prices.get("buy_mid") or prices.get("long_mid", 0)
    if buy_k is not None:
        legs.append({"action": "BUY", "type": opt_type, "strike": buy_k,
                     "expiry": expiry, "price": buy_p, "contracts": 1})
    # If the opp has a pre-built legs list (combo trades), use it directly
    if not legs and opp.get("legs"):
        return opp["legs"]
    return legs


def _normalise_opportunity(opp: dict[str, Any]) -> dict[str, Any]:
    """Map scanner field names → frontend API field names."""
    legs = opp.get("legs") or _synthetic_legs(opp)
    return {
        **opp,
        # scanner uses 'risk_reward'; frontend expects 'rr'
        "rr": opp.get("rr") or opp.get("risk_reward", 0),
        # ensure symbol is present (set by caller)
        "symbol": opp.get("symbol", ""),
        "legs": legs,
    }


def _normalise_result(result: dict[str, Any], symbol: str) -> dict[str, Any]:
    opps = result.get("all_opportunities") or []
    trade_structure = result.get("trade_structure") or {}
    risk_metrics    = result.get("risk_metrics") or {}
    ts_legs         = trade_structure.get("legs") or []

    # Attempt payoff chart for the best opportunity (legs from trade_structure)
    payoff_chart: dict[str, Any] | None = None
    if ts_legs:
        try:
            from app.services.chart_service import get_payoff_chart  # noqa: PLC0415
            # estimate spot from average strike
            strikes = [l.get("strike", 0) for l in ts_legs if l.get("strike")]
            spot = sum(strikes) / len(strikes) if strikes else 100.0
            payoff_chart = get_payoff_chart(ts_legs, spot)
        except Exception:
            pass

    # Build readable greek summary string
    def _greek_str(rm: dict[str, Any]) -> str:
        d = rm.get("total_delta", 0) or 0
        v = rm.get("total_vega",  0) or 0
        t = rm.get("total_theta", 0) or 0
        g = rm.get("total_gamma", 0) or 0
        return f"Δ{d:+.2f} ν{v:+.2f} θ{t:+.2f} γ{g:+.3f}"

    normalised: list[dict[str, Any]] = []
    for i, o in enumerate(opps):
        o_copy = {**o, "symbol": symbol}
        if i == 0:
            if ts_legs and not o_copy.get("legs"):
                o_copy["legs"] = ts_legs
            if risk_metrics:
                o_copy["metrics"] = risk_metrics
                o_copy["greeks"] = _greek_str(risk_metrics)
            if payoff_chart:
                o_copy["payoff_chart"] = payoff_chart
        normalised.append(_normalise_opportunity(o_copy))
    return {
        "symbol": symbol,
        "opportunities": normalised,
    }


def _apply_thresholds(result: dict[str, Any], thresholds: Any) -> dict[str, Any]:
    """Post-filter opportunities by user thresholds from the settings store."""
    if thresholds is None:
        return result
    min_rr   = getattr(thresholds, 'min_risk_reward', 1.5)
    min_conf = getattr(thresholds, 'min_confidence',  0.15)
    opps = result.get("opportunities", [])
    result["opportunities"] = [
        o for o in opps
        if (o.get("rr") or o.get("risk_reward", 0)) >= min_rr
        and o.get("confidence", 0) >= min_conf
    ]
    return result


async def run_symbol_scan(scanner: Any, symbol: str, thresholds: Any = None) -> dict[str, Any]:
    """Single-symbol scan, non-blocking."""
    result = await asyncio.to_thread(safe_run, scanner.scan_symbol_full, symbol, ticker=symbol)
    if result is None:
        return {"symbol": symbol, "opportunities": [], "error": "Scan failed"}
    normalised = _normalise_result(result, symbol)
    return _apply_thresholds(normalised, thresholds)


async def start_universe_scan(scanner: Any, symbols: list[str], thresholds: Any = None) -> str:
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
                row = _apply_thresholds(_normalise_result(result, sym), thresholds)
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
