from __future__ import annotations

import asyncio
import traceback

from fastapi import APIRouter, Query

from app.dependencies import get_vix_engine
from app.services.vix_service import get_vix_data
from app.services.chart_service import get_vix_charts

router = APIRouter()


@router.get("")
async def get_vix() -> dict:
    try:
        vix_engine = get_vix_engine()
        return await get_vix_data(vix_engine)
    except Exception as exc:
        traceback.print_exc()
        return {"error": str(exc), "traceback": traceback.format_exc()}


@router.get("/snapshot")
async def get_vix_snapshot(date: str = Query(..., description="YYYY-MM-DD")) -> dict:
    """Return VIX metrics + charts as-of a specific historical date."""
    try:
        vix_engine = get_vix_engine()
        raw = await asyncio.to_thread(vix_engine.get_snapshot, date)
        if raw.get("error"):
            return raw
        charts = get_vix_charts(raw)
        return {**raw, "charts": charts}
    except Exception as exc:
        traceback.print_exc()
        return {"error": str(exc), "traceback": traceback.format_exc()}
