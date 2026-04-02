from __future__ import annotations

import asyncio
import logging
from typing import Any

from app.services.chart_service import get_vix_charts

logger = logging.getLogger("vegaplex.vix")


async def get_vix_data(vix_engine: Any, regime_result: dict | None = None) -> dict[str, Any]:
    """Fetch full VIX data set + build all charts.  Non-blocking."""
    raw = await asyncio.to_thread(vix_engine.get_all, regime_result=regime_result)
    if not raw:
        return {"error": "VIX data unavailable"}

    charts = get_vix_charts(raw)
    return {**raw, "charts": charts}
