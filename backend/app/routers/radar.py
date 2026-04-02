from __future__ import annotations

import asyncio
from typing import Annotated, Any

from fastapi import APIRouter, Depends

from app.dependencies import get_scanner, get_skew_history
from app.models.requests import RadarRequest
from app.services.chart_service import get_radar_charts

router = APIRouter()


@router.post("")
async def get_radar(
    body: RadarRequest,
    skew_history: Annotated[Any, Depends(get_skew_history)],
) -> dict:
    symbols = [s.upper() for s in body.symbols]
    radar_data = await asyncio.to_thread(
        skew_history.get_universe_state, symbols, lookback=body.lookback
    )
    if not radar_data:
        return {"error": "No radar data"}

    charts = get_radar_charts(radar_data)
    return {**radar_data, "charts": charts}
