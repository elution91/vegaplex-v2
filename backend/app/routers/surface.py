from __future__ import annotations

import asyncio
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query

from app.dependencies import get_scanner
from app.services.chart_service import get_smile_chart, get_surface_charts

router = APIRouter()


@router.get("/{symbol}")
async def get_surface(
    symbol: str,
    scanner: Annotated[Any, Depends(get_scanner)],
) -> dict:
    """Full surface data (raw)."""
    result = await asyncio.to_thread(scanner.scan_symbol_full, symbol.upper())
    return result or {}


@router.get("/{symbol}/charts")
async def get_surface_charts_endpoint(
    symbol: str,
    option_type: str = Query(default="call", pattern="^(call|put)$"),
    scanner: Annotated[Any, Depends(get_scanner)],
) -> dict:
    """Surface data + all chart option dicts."""
    surface_data = await asyncio.to_thread(scanner.scan_symbol_full, symbol.upper())
    if not surface_data:
        return {"error": "No surface data"}

    charts = get_surface_charts(surface_data, option_type=option_type)
    expiries = list(surface_data.get("expiries", {}).keys())
    return {
        "symbol": symbol.upper(),
        "expiries": expiries,
        **charts,
    }


@router.get("/{symbol}/smile")
async def get_smile(
    symbol: str,
    expiry: str = Query(...),
    scanner: Annotated[Any, Depends(get_scanner)],
) -> dict:
    surface_data = await asyncio.to_thread(scanner.scan_symbol_full, symbol.upper())
    if not surface_data:
        return {"error": "No surface data"}
    return get_smile_chart(surface_data, expiry)
