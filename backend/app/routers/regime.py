from __future__ import annotations

import asyncio

from fastapi import APIRouter

from app.dependencies import get_scanner, get_skew_history
from app.models.requests import RegimeClassifyRequest, RegimeUniverseRequest
from app.services.chart_service import get_regime_charts

router = APIRouter()


@router.post("/classify")
async def classify_regime(body: RegimeClassifyRequest) -> dict:
    result = await asyncio.to_thread(
        get_scanner().regime_classifier.classify,
        body.symbol.upper(),
        lookback=body.lookback,
    )
    return result or {"error": "Classification failed"}


@router.post("/classify-universe")
async def classify_universe(body: RegimeUniverseRequest) -> dict:
    results = await asyncio.to_thread(
        get_scanner().regime_classifier.classify_universe,
        [s.upper() for s in body.symbols],
        lookback=body.lookback,
    )
    return {"results": results or []}


@router.get("/{symbol}/charts")
async def regime_charts(symbol: str) -> dict:
    scanner = get_scanner()
    sym = symbol.upper()
    regime_result, skew_ctx = await asyncio.gather(
        asyncio.to_thread(scanner.regime_classifier.classify, sym),
        asyncio.to_thread(get_skew_history().get_context, sym),
    )
    if not regime_result:
        return {"error": "No regime data"}
    charts = get_regime_charts(regime_result)

    # Attach IV/RV percentiles from skew history for badge display
    pcts: dict = {}
    if skew_ctx:
        rv21 = skew_ctx.get("rv_21d")
        if rv21:
            pcts["rv_percentile"] = rv21.get("percentile")
        atm_call = skew_ctx.get("call_atm_vol")
        if atm_call:
            pcts["atm_iv_percentile"] = atm_call.get("percentile")
            pcts["atm_iv"] = atm_call.get("current")

    return {"symbol": sym, "regime": regime_result, "charts": charts, **pcts}
