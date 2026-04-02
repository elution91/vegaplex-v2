from __future__ import annotations

import asyncio
from typing import Annotated, Any

from fastapi import APIRouter, Depends

from app.dependencies import get_scanner
from app.models.requests import RegimeClassifyRequest, RegimeUniverseRequest
from app.services.chart_service import get_regime_charts

router = APIRouter()


@router.post("/classify")
async def classify_regime(
    body: RegimeClassifyRequest,
    scanner: Annotated[Any, Depends(get_scanner)],
) -> dict:
    result = await asyncio.to_thread(
        scanner.regime_classifier.classify,
        body.symbol.upper(),
        lookback=body.lookback,
    )
    return result or {"error": "Classification failed"}


@router.post("/classify-universe")
async def classify_universe(
    body: RegimeUniverseRequest,
    scanner: Annotated[Any, Depends(get_scanner)],
) -> dict:
    results = await asyncio.to_thread(
        scanner.regime_classifier.classify_universe,
        [s.upper() for s in body.symbols],
        lookback=body.lookback,
    )
    return {"results": results or []}


@router.get("/{symbol}/charts")
async def regime_charts(
    symbol: str,
    scanner: Annotated[Any, Depends(get_scanner)],
) -> dict:
    regime_result = await asyncio.to_thread(
        scanner.regime_classifier.classify, symbol.upper()
    )
    if not regime_result:
        return {"error": "No regime data"}

    price_history = await asyncio.to_thread(scanner.get_price_history, symbol.upper())
    charts = get_regime_charts(regime_result, price_history)
    return {"symbol": symbol.upper(), "regime": regime_result, "charts": charts}
