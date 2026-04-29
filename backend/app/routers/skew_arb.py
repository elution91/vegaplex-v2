"""
Cross-Ticker Relative Skew Arb Router

Compares √T-normalised skew slopes between two related tickers
(e.g. TQQQ/QQQ, UPRO/SPY, UVXY/SVXY) and computes the spread
z-score relative to a rolling window.

Bennett pp.154-169: regime-normalised skew comparison.
"""
from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import numpy as np
from fastapi import APIRouter, Query

from app.dependencies import get_scanner, get_skew_history

logger = logging.getLogger("vegaplex.skew_arb")

router = APIRouter()

# Predefined structurally-related pairs
PRESET_PAIRS = [
    {"label": "TQQQ / QQQ",   "a": "TQQQ", "b": "QQQ"},
    {"label": "UPRO / SPY",   "a": "UPRO", "b": "SPY"},
    {"label": "SQQQ / QQQ",   "a": "SQQQ", "b": "QQQ"},
    {"label": "SPXU / SPY",   "a": "SPXU", "b": "SPY"},
    {"label": "UVXY / SVXY",  "a": "UVXY", "b": "SVXY"},
    {"label": "IWM / SPY",    "a": "IWM",  "b": "SPY"},
    {"label": "GLD / SLV",    "a": "GLD",  "b": "SLV"},
]


def _extract_norm_skew(scan_result: dict[str, Any]) -> dict[str, float] | None:
    """
    Extract √T-normalised put skew slope per expiry from a scan result.
    Returns {expiry: normalised_slope} or None on failure.
    """
    skew_metrics = scan_result.get("skew_metrics", {})
    by_expiry = skew_metrics.get("put_skew", {}).get("by_expiry", [])
    if not by_expiry:
        by_expiry = skew_metrics.get("call_skew", {}).get("by_expiry", [])
    if not by_expiry:
        return None

    result = {}
    for e in by_expiry:
        tte = e.get("tte")
        slope = e.get("slope")
        expiry = str(e.get("expiry", ""))
        if tte and slope is not None and tte > 0:
            result[expiry] = float(slope) / (float(tte) ** 0.5)
    return result if result else None


def _nearest_common_tte(norm_a: dict, norm_b: dict) -> tuple[float | None, float | None]:
    """Match expiries by proximity and return the nearest common normalised slopes."""
    if not norm_a or not norm_b:
        return None, None
    # Use the first expiry (shortest tenor) from each — closest to ATM skew edge
    slopes_a = list(norm_a.values())
    slopes_b = list(norm_b.values())
    if not slopes_a or not slopes_b:
        return None, None
    return slopes_a[0], slopes_b[0]


def _build_pair_chart(
    sym_a: str, sym_b: str,
    slope_a: float, slope_b: float,
    spread: float, z_score: float | None,
    hist_a: list, hist_b: list, hist_spread: list,
    current_bucket_pct: float | None,
) -> dict[str, Any]:
    """Build ECharts option for the skew spread chart."""
    # Spread history series
    spread_series = {
        "type": "line", "name": "Spread (A−B)",
        "data": [[i, round(v, 4)] for i, v in enumerate(hist_spread)],
        "symbol": "none",
        "lineStyle": {"color": "#FACC15", "width": 2},
        "areaStyle": {"color": "rgba(250,204,21,0.06)"},
        "yAxisIndex": 0,
    }
    # Z-score line (dashed)
    mean_spread = float(np.mean(hist_spread)) if hist_spread else 0.0
    std_spread  = float(np.std(hist_spread))  if hist_spread else 1.0

    mark_lines = []
    if z_score is not None:
        for z_val, color in [(1.5, "#f85149"), (-1.5, "#3fb950"), (0, "#484f58")]:
            mark_lines.append({
                "yAxis": round(mean_spread + z_val * std_spread, 4),
                "lineStyle": {"color": color, "type": "dashed", "width": 1},
                "label": {
                    "show": True,
                    "formatter": f"z={z_val:+.1f}" if z_val != 0 else "mean",
                    "color": color, "fontSize": 10,
                },
            })

    if mark_lines:
        spread_series["markLine"] = {
            "silent": True,
            "symbol": "none",
            "data": [{"yAxis": m["yAxis"], "lineStyle": m["lineStyle"], "label": m["label"]}
                     for m in mark_lines],
        }

    return {
        "grid": {"top": 36, "bottom": 48, "left": 48, "right": 16},
        "xAxis": {"type": "category", "show": False},
        "yAxis": {
            "type": "value",
            "name": "√T-norm skew spread",
            "nameTextStyle": {"fontSize": 10},
            "splitLine": {"lineStyle": {"color": "#161b22", "type": "dashed"}},
        },
        "tooltip": {
            "trigger": "axis",
            "formatter": "function(p){return p[0].value[1].toFixed(4);}",
        },
        "series": [spread_series],
    }


@router.get("/pairs")
async def get_preset_pairs() -> list[dict]:
    return PRESET_PAIRS


@router.get("/compare")
async def compare_pair(
    a: str = Query(..., description="Symbol A (e.g. TQQQ)"),
    b: str = Query(..., description="Symbol B (e.g. QQQ)"),
    lookback: int = Query(default=21, ge=5, le=90, description="Z-score lookback window"),
) -> dict:
    sym_a, sym_b = a.upper(), b.upper()
    scanner = get_scanner()
    skew_hist = get_skew_history()

    # Fetch both surfaces in parallel
    def _scan(sym: str):
        try:
            return sym, scanner.scan_symbol_full(sym)
        except Exception as e:
            logger.warning(f"Scan failed for {sym}: {e}")
            return sym, None

    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = {pool.submit(_scan, s): s for s in (sym_a, sym_b)}
        results: dict[str, Any] = {}
        for fut in as_completed(futs):
            sym, res = fut.result()
            results[sym] = res

    res_a = results.get(sym_a)
    res_b = results.get(sym_b)

    if not res_a or not res_b:
        missing = sym_a if not res_a else sym_b
        return {"error": f"No surface data for {missing}"}

    norm_a = _extract_norm_skew(res_a)
    norm_b = _extract_norm_skew(res_b)

    if not norm_a or not norm_b:
        missing = []
        if not norm_a: missing.append(sym_a)
        if not norm_b: missing.append(sym_b)
        return {"error": f"No valid skew data for {', '.join(missing)} — options may be unavailable or IVs stale (market closed?)"}

    slope_a, slope_b = _nearest_common_tte(norm_a, norm_b)
    if slope_a is None or slope_b is None:
        return {"error": "No matching expiries found"}

    spread_now = slope_a - slope_b

    # Historical skew percentiles from skew_history DB
    ctx_a = await asyncio.to_thread(skew_hist.get_context, sym_a, lookback)
    ctx_b = await asyncio.to_thread(skew_hist.get_context, sym_b, lookback)

    # Build historical spread from stored put_slope data
    def _hist_slopes(sym: str) -> list[float]:
        try:
            with skew_hist._conn() as conn:
                rows = conn.execute(
                    "SELECT put_slope FROM skew_snapshots WHERE symbol=? AND put_slope IS NOT NULL "
                    "ORDER BY ts DESC LIMIT ?",
                    (sym, lookback * 5),
                ).fetchall()
            return [r[0] for r in reversed(rows)]
        except Exception:
            return []

    hist_a_slopes, hist_b_slopes = await asyncio.gather(
        asyncio.to_thread(_hist_slopes, sym_a),
        asyncio.to_thread(_hist_slopes, sym_b),
    )

    # Align histories to same length
    n = min(len(hist_a_slopes), len(hist_b_slopes))
    hist_spread = [
        hist_a_slopes[i] - hist_b_slopes[i]
        for i in range(n)
    ] if n >= 2 else []

    # Z-score of current spread vs history
    z_score: float | None = None
    spread_pct: float | None = None
    if len(hist_spread) >= 5:
        arr = np.array(hist_spread)
        mu, sigma = float(arr.mean()), float(arr.std())
        z_score = round((spread_now - mu) / sigma, 2) if sigma > 1e-8 else 0.0
        # percentile
        spread_pct = round(float(np.mean(arr <= spread_now)) * 100, 1)

    # Signal interpretation
    signal: str
    signal_color: str
    if z_score is None:
        signal, signal_color = "Insufficient history", "#484f58"
    elif z_score >= 2.0:
        signal = f"{sym_a} skew expensive vs {sym_b} — consider fading ({sym_a} short skew / {sym_b} long skew)"
        signal_color = "#f85149"
    elif z_score <= -2.0:
        signal = f"{sym_a} skew cheap vs {sym_b} — consider buying ({sym_a} long skew / {sym_b} short skew)"
        signal_color = "#3fb950"
    elif z_score >= 1.5:
        signal = f"{sym_a} skew elevated vs {sym_b} — watch for reversion"
        signal_color = "#FACC15"
    elif z_score <= -1.5:
        signal = f"{sym_a} skew depressed vs {sym_b} — watch for reversion"
        signal_color = "#FACC15"
    else:
        signal = "Spread within normal range — no actionable divergence"
        signal_color = "#8b949e"

    chart = _build_pair_chart(
        sym_a, sym_b, slope_a, slope_b, spread_now,
        z_score, list(norm_a.values()), list(norm_b.values()),
        hist_spread, spread_pct,
    )

    return {
        "sym_a": sym_a,
        "sym_b": sym_b,
        "slope_a": round(slope_a, 4),
        "slope_b": round(slope_b, 4),
        "spread": round(spread_now, 4),
        "z_score": z_score,
        "spread_pct": spread_pct,
        "signal": signal,
        "signal_color": signal_color,
        "norm_skew_a": norm_a,
        "norm_skew_b": norm_b,
        "hist_spread_n": n,
        "chart": chart,
    }
